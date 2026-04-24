"""Unit tests for pipeline CLI helpers and run_pipeline wiring."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.cli import (
    _resolve_source_name_from_blob,
    _gcs_checkpoint_source_file,
    _create_gcs_checkpoint,
    run_pipeline,
)


class TestResolveSourceName:
    def test_simple_date_path(self):
        assert _resolve_source_name_from_blob("usda/2026/04/20/part_0000.jsonl") == "usda"

    def test_nested_sub_type(self):
        assert _resolve_source_name_from_blob(
            "usda/bulk/2026/04/21/branded/part_0000.jsonl"
        ) == "usda/branded"

    def test_survey_subdir(self):
        assert _resolve_source_name_from_blob(
            "usda/bulk/2026/04/21/survey/part_0000.jsonl"
        ) == "usda/survey"

    def test_off(self):
        assert _resolve_source_name_from_blob("off/2026/04/21/part_0000.jsonl") == "off"

    def test_esci(self):
        assert _resolve_source_name_from_blob("esci/2024/01/01/part_0000.jsonl") == "esci"

    def test_no_year_fallback(self):
        assert _resolve_source_name_from_blob("weird/path/file.jsonl") == "weird"


class TestGcsCheckpointSourceFile:
    def test_returns_deterministic_path(self):
        p1 = _gcs_checkpoint_source_file("gs://b/x/*.jsonl")
        p2 = _gcs_checkpoint_source_file("gs://b/x/*.jsonl")
        assert p1 == p2
        assert p1.name.startswith("gcs_")
        assert p1.suffix == ".jsonl"

    def test_different_uris_different(self):
        p1 = _gcs_checkpoint_source_file("gs://b/a/*.jsonl")
        p2 = _gcs_checkpoint_source_file("gs://b/b/*.jsonl")
        assert p1 != p2


class TestCreateGcsCheckpoint:
    def test_creates_entry(self, tmp_path):
        from src.pipeline.checkpoint import CheckpointManager
        mgr = CheckpointManager(checkpoint_dir=tmp_path / "ckpt")
        run_id = _create_gcs_checkpoint(mgr, "gs://bucket/usda/2026/04/20/*.jsonl")
        assert isinstance(run_id, str)
        loaded = mgr.load_checkpoint(run_id)
        assert loaded["source_file"] == "gs://bucket/usda/2026/04/20/*.jsonl"


class TestRunPipeline:
    def test_missing_local_source_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            run_pipeline(source_path=str(tmp_path / "missing.csv"), domain="nutrition")

    def test_runs_with_local_source(self, tmp_path):
        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        fake_graph = MagicMock()
        fake_graph.invoke.return_value = {
            "working_df": [1, 2, 3],
            "dq_score_pre": 0.5,
            "dq_score_post": 0.9,
            "block_sequence": ["a", "b"],
        }
        with patch("src.pipeline.cli.build_graph", return_value=fake_graph), \
             patch("src.pipeline.cli.CheckpointManager") as mgr_cls, \
             patch("src.pipeline.cli.is_gcs_uri", return_value=False):
            mgr = MagicMock()
            mgr.create.return_value = "rid-1"
            mgr_cls.return_value = mgr
            result = run_pipeline(source_path=str(src), domain="nutrition")
        assert result["dq_score_post"] == 0.9
        fake_graph.invoke.assert_called_once()

    def test_force_fresh_calls_clear(self, tmp_path):
        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        fake_graph = MagicMock()
        fake_graph.invoke.return_value = {"working_df": [], "block_sequence": []}
        with patch("src.pipeline.cli.build_graph", return_value=fake_graph), \
             patch("src.pipeline.cli.CheckpointManager") as mgr_cls, \
             patch("src.pipeline.cli.is_gcs_uri", return_value=False):
            mgr = MagicMock()
            mgr.create.return_value = "rid"
            mgr_cls.return_value = mgr
            run_pipeline(source_path=str(src), domain="nutrition", force_fresh=True)
            mgr.force_fresh.assert_called_once()

    def test_resume_with_valid_checkpoint(self, tmp_path):
        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        fake_graph = MagicMock()
        fake_graph.invoke.return_value = {"working_df": [], "block_sequence": []}
        with patch("src.pipeline.cli.build_graph", return_value=fake_graph), \
             patch("src.pipeline.cli.CheckpointManager") as mgr_cls, \
             patch("src.pipeline.cli.is_gcs_uri", return_value=False):
            mgr = MagicMock()
            mgr.get_resume_state.return_value = {"run_id": "old", "chunks": []}
            mgr.validate_checkpoint.return_value = (True, "ok")
            mgr_cls.return_value = mgr
            run_pipeline(source_path=str(src), domain="nutrition", resume=True)
            mgr.create.assert_not_called()

    def test_resume_invalid_starts_fresh(self, tmp_path):
        src = tmp_path / "src.csv"
        src.write_text("a,b\n1,2\n")
        fake_graph = MagicMock()
        fake_graph.invoke.return_value = {"working_df": [], "block_sequence": []}
        with patch("src.pipeline.cli.build_graph", return_value=fake_graph), \
             patch("src.pipeline.cli.CheckpointManager") as mgr_cls, \
             patch("src.pipeline.cli.is_gcs_uri", return_value=False):
            mgr = MagicMock()
            mgr.get_resume_state.return_value = {"run_id": "old", "chunks": []}
            mgr.validate_checkpoint.return_value = (False, "mismatch")
            mgr_cls.return_value = mgr
            run_pipeline(source_path=str(src), domain="nutrition", resume=True)
            mgr.force_fresh.assert_called()

    def test_gcs_source(self):
        fake_graph = MagicMock()
        fake_graph.invoke.return_value = {"working_df": [], "block_sequence": []}
        with patch("src.pipeline.cli.build_graph", return_value=fake_graph), \
             patch("src.pipeline.cli.CheckpointManager") as mgr_cls, \
             patch("src.pipeline.cli.is_gcs_uri", return_value=True), \
             patch("src.pipeline.cli._create_gcs_checkpoint", return_value="rid-gcs"):
            mgr = MagicMock()
            mgr_cls.return_value = mgr
            run_pipeline(source_path="gs://b/usda/2026/04/20/part.jsonl", domain="nutrition")
            fake_graph.invoke.assert_called_once()
