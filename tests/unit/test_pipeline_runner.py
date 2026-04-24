"""Unit tests for PipelineRunner."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.runner import PipelineRunner, _compute_block_dq, NULL_RATE_COLUMNS


class FakeBlock:
    def __init__(self, name="noop", outputs=None):
        self.name = name
        self.outputs = outputs or []

    def run(self, df, config):
        return df

    def audit_entry(self, rows_in, rows_out):
        return {"block": self.name, "rows_in": rows_in, "rows_out": rows_out}


@pytest.fixture
def fake_registry():
    reg = MagicMock()
    reg.blocks = {"DYNAMIC_MAPPING_src": MagicMock(domain="nutrition")}
    reg.is_stage = MagicMock(return_value=False)
    reg.expand_stage = MagicMock(return_value=[])
    reg.get = MagicMock(return_value=FakeBlock())
    return reg


class TestComputeBlockDq:
    def test_empty_df(self):
        assert _compute_block_dq(pd.DataFrame()) == 0.0

    def test_no_cols(self):
        assert _compute_block_dq(pd.DataFrame({"x": [1, 2]})) == 0.0

    def test_full(self):
        df = pd.DataFrame({"product_name": ["a", "b"], "brand_name": ["x", None]})
        v = _compute_block_dq(df)
        assert 0 <= v <= 1


class TestRun:
    def test_run_basic(self, fake_registry):
        r = PipelineRunner(fake_registry)
        df = pd.DataFrame({"a": [1, 2]})
        out, log = r.run(df, ["b1"])
        assert len(out) == 2
        assert any(e.get("block") == "noop" for e in log)

    def test_column_mapping_applied(self, fake_registry):
        r = PipelineRunner(fake_registry)
        df = pd.DataFrame({"src_col": [1, 2]})
        out, log = r.run(df, ["b1"], column_mapping={"src_col": "product_name"})
        assert "product_name" in out.columns
        assert any(e.get("block") == "column_mapping" for e in log)

    def test_column_mapping_dedupe(self, fake_registry):
        r = PipelineRunner(fake_registry)
        df = pd.DataFrame({"a": [1], "b": [2]})
        out, _ = r.run(df, ["b1"], column_mapping={"a": "x", "b": "x"})
        assert list(out.columns).count("x") == 1

    def test_missing_block_raises(self, fake_registry):
        fake_registry.get.side_effect = KeyError("nope")
        r = PipelineRunner(fake_registry)
        with pytest.raises(RuntimeError):
            r.run(pd.DataFrame({"a": [1]}), ["missing"])

    def test_expand_generated_sentinel(self, fake_registry):
        r = PipelineRunner(fake_registry)
        expanded = r._expand_sequence(["__generated__"], domain="nutrition")
        assert "DYNAMIC_MAPPING_src" in expanded

    def test_expand_stage(self, fake_registry):
        fake_registry.is_stage.side_effect = lambda s: s == "dedup_stage"
        fake_registry.expand_stage.return_value = ["a", "b"]
        r = PipelineRunner(fake_registry)
        out = r._expand_sequence(["dedup_stage", "x"])
        assert out == ["a", "b", "x"]

    def test_validate_schema_coverage_none(self, fake_registry):
        r = PipelineRunner(fake_registry)
        assert r._validate_schema_coverage(["a"], {}, {}) == []

    def test_validate_schema_coverage_missing(self, fake_registry):
        schema = MagicMock()
        schema.required_columns = {"product_name", "brand_name"}
        fake_registry.get.return_value = FakeBlock(outputs=["product_name"])
        r = PipelineRunner(fake_registry)
        warns = r._validate_schema_coverage(["a"], {}, {"unified_schema": schema})
        assert any("brand_name" in w for w in warns)


class TestRunChunked:
    def test_chunked_empty_returns_empty(self, fake_registry, tmp_path):
        r = PipelineRunner(fake_registry)
        csv = tmp_path / "empty.csv"
        csv.write_text("a\n")
        out, log = r.run_chunked(
            str(csv), ["b1"], config={"output_dir": str(tmp_path), "run_id": "r1"}
        )
        assert out.empty or len(out) == 0

    def test_chunked_writes_parquet(self, fake_registry, tmp_path):
        r = PipelineRunner(fake_registry)
        csv = tmp_path / "f.csv"
        csv.write_text("a,b\n1,2\n3,4\n")
        out, log = r.run_chunked(
            str(csv), ["b1"],
            config={"output_dir": str(tmp_path), "run_id": "r1"},
            chunk_size=1,
        )
        assert len(out) == 2
        assert len(log) == 2
