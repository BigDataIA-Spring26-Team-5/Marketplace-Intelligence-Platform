"""UC2 observability integration tests — all UC2 services mocked."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from src.blocks.fuzzy_deduplicate import FuzzyDeduplicateBlock
from src.models.llm import reset_llm_counter, get_llm_call_count
from src.pipeline.runner import NULL_RATE_COLUMNS, PipelineRunner
from src.registry.block_registry import BlockRegistry


# ── Helpers ──────────────────────────────────────────────────────────


def _make_df(n: int = 5, null_product: bool = False) -> pd.DataFrame:
    data = {
        "product_name": [None if (null_product and i == 0) else f"product {i}" for i in range(n)],
        "brand_name": [f"brand {i}" for i in range(n)],
        "ingredients": [f"ing {i}" for i in range(n)],
        "primary_category": ["snacks"] * n,
    }
    return pd.DataFrame(data)


def _registry_with_noop_blocks(*names: str) -> BlockRegistry:
    """Return a BlockRegistry pre-populated with minimal no-op blocks."""
    from src.blocks.base import Block

    class NoopBlock(Block):
        domain = "all"
        description = "noop"
        inputs: list[str] = []
        outputs: list[str] = []

        def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
            return df

    reg = BlockRegistry()
    for n in names:
        b = NoopBlock()
        b.name = n
        reg.register_block(b)
    return reg


# ── T019: block events emitted ────────────────────────────────────────


def test_block_events_emitted() -> None:
    """block_start + block_end emitted once each per block (2 calls per block)."""
    mock_emit = MagicMock()
    reg = _registry_with_noop_blocks("block_a", "block_b", "block_c")
    runner = PipelineRunner(reg)

    with (
        patch("src.pipeline.runner._UC2_AVAILABLE", True),
        patch("src.pipeline.runner._emit_event", mock_emit),
    ):
        runner.run(_make_df(), ["block_a", "block_b", "block_c"], config={"run_id": "test-run", "source_name": "src"})

    event_types = [c.args[0]["event_type"] for c in mock_emit.call_args_list]
    assert event_types.count("block_start") == 3
    assert event_types.count("block_end") == 3
    assert mock_emit.call_count == 6


# ── T020: events suppressed when UC2 unavailable ─────────────────────


def test_block_events_suppressed_when_unavailable() -> None:
    """No emit calls when _UC2_AVAILABLE is False."""
    mock_emit = MagicMock()
    reg = _registry_with_noop_blocks("block_a")
    runner = PipelineRunner(reg)

    with (
        patch("src.pipeline.runner._UC2_AVAILABLE", False),
        patch("src.pipeline.runner._emit_event", mock_emit),
    ):
        runner.run(_make_df(), ["block_a"], config={"run_id": "test-run", "source_name": "src"})

    mock_emit.assert_not_called()


# ── T021: run lifecycle success ───────────────────────────────────────


def test_run_lifecycle_success(tmp_path: pytest.FixtureRequest) -> None:
    """run_started emitted first, run_completed(status='success') emitted last."""
    df = _make_df()
    csv_path = tmp_path / "test.csv"  # type: ignore[operator]
    df.to_csv(csv_path, index=False)

    mock_emit = MagicMock()

    from src.agents.graph import run_pipeline_node

    state = {
        "source_path": str(csv_path),
        "block_sequence": [],
        "column_mapping": {},
        "domain": "nutrition",
        "enable_enrichment": False,
        "chunk_size": 100,
        "source_sep": ",",
    }

    with (
        patch("src.agents.graph._UC2_AVAILABLE", True),
        patch("src.agents.graph._emit_event", mock_emit),
        patch("src.pipeline.runner.PipelineRunner.run_chunked", return_value=(df, [])),
    ):
        run_pipeline_node(state)  # type: ignore[arg-type]

    types = [c.args[0]["event_type"] for c in mock_emit.call_args_list]
    assert "run_started" in types
    assert "run_completed" in types
    completed = [c.args[0] for c in mock_emit.call_args_list if c.args[0]["event_type"] == "run_completed"]
    assert completed[-1]["status"] == "success"


# ── T022: run_completed fires even on exception ───────────────────────


def test_run_completed_on_exception(tmp_path: pytest.FixtureRequest) -> None:
    """run_completed(status='failed') must fire even when run_chunked raises."""
    df = _make_df()
    csv_path = tmp_path / "test.csv"  # type: ignore[operator]
    df.to_csv(csv_path, index=False)

    mock_emit = MagicMock()

    from src.agents.graph import run_pipeline_node

    state = {
        "source_path": str(csv_path),
        "block_sequence": ["normalize_text"],
        "column_mapping": {},
        "domain": "nutrition",
        "enable_enrichment": False,
        "chunk_size": 100,
        "source_sep": ",",
    }

    with (
        patch("src.agents.graph._UC2_AVAILABLE", True),
        patch("src.agents.graph._emit_event", mock_emit),
        patch("src.pipeline.runner.PipelineRunner.run_chunked", side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            run_pipeline_node(state)  # type: ignore[arg-type]

    types = [c.args[0]["event_type"] for c in mock_emit.call_args_list]
    assert "run_completed" in types
    completed_events = [c.args[0] for c in mock_emit.call_args_list if c.args[0]["event_type"] == "run_completed"]
    assert completed_events[-1]["status"] == "failed"


# ── T023: quarantine events ───────────────────────────────────────────


def test_quarantine_events(tmp_path: pytest.FixtureRequest) -> None:
    """Quarantine events emitted with reason and trimmed row_data (not full row)."""
    # Build a df where product_name is NaN so it fails required-field check
    df = pd.DataFrame({
        "product_name": [float("nan"), "valid product"],
        "brand_name": ["brand A", "brand B"],
        "ingredients": ["ing A", "ing B"],
        "primary_category": ["snacks", "snacks"],
    })

    mock_emit = MagicMock()

    from src.agents.graph import run_pipeline_node

    state: dict = {
        "source_path": str(tmp_path / "src.csv"),
        "block_sequence": [],
        "column_mapping": {},
        "domain": "nutrition",
        "enable_enrichment": False,
        "chunk_size": 100,
        "source_sep": ",",
    }

    with (
        patch("src.agents.graph._UC2_AVAILABLE", True),
        patch("src.agents.graph._emit_event", mock_emit),
        patch("src.pipeline.runner.PipelineRunner.run_chunked", return_value=(df, [])),
    ):
        run_pipeline_node(state)  # type: ignore[arg-type]

    quarantine_calls = [
        c.args[0] for c in mock_emit.call_args_list
        if c.args[0]["event_type"] == "quarantine"
    ]
    assert len(quarantine_calls) >= 1
    evt = quarantine_calls[0]
    assert "product_name" in evt["reason"]
    # row_data must contain the 3 key fields
    assert "product_name" in evt["row_data"]
    assert "brand_name" in evt["row_data"]
    assert "ingredients" in evt["row_data"]
    # row_data must NOT be the full row (no extra pipeline-internal columns beyond key fields + offending)
    allowed = {"product_name", "brand_name", "ingredients", "primary_category"}
    assert set(evt["row_data"].keys()) <= allowed


# ── T024: dedup cluster populated ────────────────────────────────────


def test_dedup_cluster_populated() -> None:
    """FuzzyDeduplicateBlock.last_clusters non-empty and every entry has size > 1."""
    data = {
        "product_name": ["cheerios original", "cheerios original 12oz", "cheerios", "apple juice", "apple juice fresh"],
        "brand_name": ["General Mills", "General Mills", "General Mills", "Tropicana", "Tropicana"],
    }
    df = pd.DataFrame(data)

    block = FuzzyDeduplicateBlock()
    block.run(df, config={"dedup_threshold": 70})

    assert isinstance(block.last_clusters, list)
    assert len(block.last_clusters) >= 1
    for cluster in block.last_clusters:
        assert cluster["size"] > 1
        assert "cluster_id" in cluster
        assert "member_product_names" in cluster
        assert "canonical_product_name" in cluster


# ── T025: metrics push called with all 15 keys ────────────────────────


def test_metrics_push_called(tmp_path: pytest.FixtureRequest) -> None:
    """save_output_node calls MetricsCollector().push() with all 15 required keys."""
    df = _make_df()
    output_path = tmp_path / "out"  # type: ignore[operator]
    output_path.mkdir()

    mock_collector_instance = MagicMock()
    mock_collector_cls = MagicMock(return_value=mock_collector_instance)

    from src.agents.graph import save_output_node

    state = {
        "source_path": str(tmp_path / "src.csv"),
        "working_df": df,
        "source_df": df,
        "dq_score_pre": 0.7,
        "dq_score_post": 0.85,
        "enrichment_stats": {"deterministic": 2, "embedding": 1, "llm": 1},
        "_run_id": "test-uuid",
        "_run_start_time": time.perf_counter() - 1.0,
    }

    required_keys = {
        "rows_in", "rows_out", "dq_score_pre", "dq_score_post", "dq_delta",
        "null_rate", "dedup_rate", "s1_count", "s2_count", "s3_count", "s4_count",
        "cost_usd", "llm_calls", "quarantine_rows", "block_duration_seconds",
    }

    with (
        patch("src.agents.graph._UC2_AVAILABLE", True),
        patch("src.agents.graph._MetricsCollector", mock_collector_cls),
        patch("src.agents.graph.OUTPUT_DIR", output_path),
    ):
        save_output_node(state)  # type: ignore[arg-type]

    mock_collector_instance.push.assert_called_once()
    call_kwargs = mock_collector_instance.push.call_args
    metrics_arg = call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("metrics", {})
    assert required_keys <= set(metrics_arg.keys()), f"Missing keys: {required_keys - set(metrics_arg.keys())}"


# ── T026: LLM counter lifecycle ───────────────────────────────────────


def test_llm_counter_lifecycle() -> None:
    """Counter increments per call_llm(), resets to 0 via reset_llm_counter()."""
    reset_llm_counter()
    assert get_llm_call_count() == 0

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "hello"

    with patch("src.models.llm.litellm.completion", return_value=mock_response):
        from src.models.llm import call_llm
        call_llm("test-model", [{"role": "user", "content": "hi"}])
        call_llm("test-model", [{"role": "user", "content": "hi"}])

    assert get_llm_call_count() == 2

    reset_llm_counter()
    assert get_llm_call_count() == 0
