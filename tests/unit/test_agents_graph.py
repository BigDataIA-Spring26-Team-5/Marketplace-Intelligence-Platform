"""Unit tests for agents/graph — routing, plan_sequence_node, run_step."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.agents import graph as graph_mod
from src.agents.graph import (
    route_after_analyze_schema,
    plan_sequence_node,
    run_step,
    build_graph,
    NODE_MAP,
    _sanitize_for_json,
)


class TestRouteAfterAnalyze:
    def test_cache_hit_skips_critic(self):
        state = {"cache_yaml_hit": True, "with_critic": True}
        assert route_after_analyze_schema(state) == "check_registry"

    def test_without_critic_skips(self):
        state = {"with_critic": False}
        assert route_after_analyze_schema(state) == "check_registry"

    def test_with_critic_enabled(self):
        state = {"with_critic": True}
        assert route_after_analyze_schema(state) == "critique_schema"

    def test_default_skips(self):
        assert route_after_analyze_schema({}) == "check_registry"


class TestSanitizeForJson:
    def test_nan_becomes_none(self):
        assert _sanitize_for_json(float("nan")) is None

    def test_inf_becomes_none(self):
        assert _sanitize_for_json(float("inf")) is None

    def test_nested_dict(self):
        out = _sanitize_for_json({"a": float("nan"), "b": [1, float("inf")]})
        assert out == {"a": None, "b": [1, None]}

    def test_normal_values_preserved(self):
        assert _sanitize_for_json({"x": 1, "y": "ok"}) == {"x": 1, "y": "ok"}


class TestPlanSequenceNode:
    def test_skips_if_sequence_exists(self):
        state = {"block_sequence": ["a", "b"]}
        assert plan_sequence_node(state) == {}

    @patch("src.agents.graph.call_llm_json")
    @patch("src.agents.graph.get_orchestrator_llm")
    def test_forces_mandatory_blocks(self, mock_llm, mock_call):
        mock_llm.return_value = "fake-model"
        mock_call.return_value = {
            "block_sequence": [],  # LLM dropped everything
            "reasoning": "test",
            "skipped_blocks": {},
        }
        state = {
            "domain": "nutrition",
            "source_schema": {"col1": {"dtype": "object"}},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "pipeline_mode": "full",
        }
        result = plan_sequence_node(state)
        seq = result["block_sequence"]
        assert "dq_score_pre" in seq
        assert "__generated__" in seq
        assert "dedup_stage" in seq
        assert "dq_score_post" in seq

    @patch("src.agents.graph.call_llm_json")
    @patch("src.agents.graph.get_orchestrator_llm")
    def test_silver_mode_has_schema_enforce(self, mock_llm, mock_call):
        mock_llm.return_value = "m"
        mock_call.return_value = {"block_sequence": [], "reasoning": "", "skipped_blocks": {}}
        state = {
            "domain": "nutrition",
            "source_schema": {},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "pipeline_mode": "silver",
        }
        result = plan_sequence_node(state)
        assert "schema_enforce" in result["block_sequence"]
        assert "dedup_stage" not in result["block_sequence"]

    @patch("src.agents.graph.call_llm_json")
    @patch("src.agents.graph.get_orchestrator_llm")
    def test_llm_returned_full_sequence(self, mock_llm, mock_call):
        mock_llm.return_value = "m"
        mock_call.return_value = {
            "block_sequence": ["dq_score_pre", "__generated__", "strip_whitespace", "dedup_stage", "dq_score_post"],
            "reasoning": "ok",
            "skipped_blocks": {"lowercase_brand": "not needed"},
        }
        state = {"domain": "nutrition", "source_schema": {}, "gaps": [],
                 "registry_misses": [], "block_registry_hits": {}, "pipeline_mode": "full"}
        result = plan_sequence_node(state)
        assert result["sequence_reasoning"] == "ok"
        assert result["skipped_blocks"] == {"lowercase_brand": "not needed"}

    @patch("src.agents.graph.call_llm_json")
    @patch("src.agents.graph.get_orchestrator_llm")
    def test_writes_cache_when_fingerprint_present(self, mock_llm, mock_call, tmp_path):
        mock_llm.return_value = "m"
        mock_call.return_value = {"block_sequence": ["dq_score_pre"], "reasoning": "", "skipped_blocks": {}}
        cache = MagicMock()
        state = {
            "domain": "nutrition", "source_schema": {}, "gaps": [],
            "registry_misses": [], "block_registry_hits": {}, "pipeline_mode": "full",
            "_schema_fingerprint": "abc123",
            "cache_client": cache,
            "column_mapping": {"a": "b"},
            "operations": [],
        }
        plan_sequence_node(state)
        assert cache.set.called
        args = cache.set.call_args
        assert args.args[0] == "yaml"


class TestNodeMap:
    def test_node_map_contains_all_steps(self):
        expected = {"load_source", "analyze_schema", "critique_schema",
                    "check_registry", "plan_sequence", "run_pipeline", "save_output"}
        assert set(NODE_MAP.keys()) == expected


class TestRunStep:
    def test_unknown_step_raises(self):
        with pytest.raises(KeyError):
            run_step("bogus", {})

    def test_calls_node_and_updates_state(self):
        original = NODE_MAP["plan_sequence"]
        NODE_MAP["plan_sequence"] = lambda s: {"foo": "bar"}
        try:
            state = {"x": 1}
            out = run_step("plan_sequence", state)
            assert out["foo"] == "bar"
            assert out["x"] == 1
        finally:
            NODE_MAP["plan_sequence"] = original


class TestBuildGraph:
    def test_compiles(self):
        g = build_graph()
        assert g is not None
