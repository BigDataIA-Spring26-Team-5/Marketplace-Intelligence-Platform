"""Tests for Agent 3 — sequence planning logic in src/agents/graph.py.

Focuses on the reorder-only invariant: Agent 3 cannot add or remove blocks
from the pool. If the LLM drops a block, plan_sequence_node must restore it
before dq_score_post.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("langgraph")

from src.agents.graph import plan_sequence_node, route_after_analyze_schema


# ---------------------------------------------------------------------------
# route_after_analyze_schema
# ---------------------------------------------------------------------------


class TestRouteAfterAnalyzeSchema:
    def test_skips_critic_on_cache_hit(self):
        state = {"cache_yaml_hit": True, "with_critic": True}
        assert route_after_analyze_schema(state) == "check_registry"

    def test_skips_critic_when_disabled(self):
        state = {"with_critic": False}
        assert route_after_analyze_schema(state) == "check_registry"

    def test_skips_critic_when_flag_absent(self):
        state = {}
        assert route_after_analyze_schema(state) == "check_registry"

    def test_runs_critic_when_enabled(self):
        state = {"with_critic": True}
        assert route_after_analyze_schema(state) == "critique_schema"

    def test_cache_hit_overrides_critic_flag(self):
        state = {"cache_yaml_hit": True, "with_critic": True}
        # Cache hit short-circuits even with critic enabled
        assert route_after_analyze_schema(state) == "check_registry"


# ---------------------------------------------------------------------------
# plan_sequence_node
# ---------------------------------------------------------------------------


class TestPlanSequenceNode:
    def test_skip_when_block_sequence_already_set(self):
        state = {"block_sequence": ["dq_score_pre", "dq_score_post"]}
        assert plan_sequence_node(state) == {}

    def test_silver_mode_uses_silver_sequence(self):
        state = {
            "pipeline_mode": "silver",
            "domain": "nutrition",
        }
        with patch("src.agents.graph.BlockRegistry") as mock_reg:
            mock_reg.instance.return_value.get_silver_sequence.return_value = [
                "dq_score_pre",
                "DYNAMIC_MAPPING_nutrition",
            ]
            result = plan_sequence_node(state)
        assert result["block_sequence"] == ["dq_score_pre", "DYNAMIC_MAPPING_nutrition"]
        assert "silver mode" in result["sequence_reasoning"]

    def test_silver_mode_does_not_call_llm(self):
        state = {"pipeline_mode": "silver", "domain": "nutrition"}
        with patch("src.agents.graph.BlockRegistry") as mock_reg, \
             patch("src.agents.graph.call_llm_json") as mock_call:
            mock_reg.instance.return_value.get_silver_sequence.return_value = ["x"]
            plan_sequence_node(state)
            mock_call.assert_not_called()

    def test_dropped_block_appended_before_dq_score_post(self):
        # Pool contains 4 blocks; LLM only returns 3 — missing one must be re-injected
        pool = ["dq_score_pre", "fuzzy_deduplicate", "llm_enrich", "dq_score_post"]
        llm_sequence = ["dq_score_pre", "fuzzy_deduplicate", "dq_score_post"]

        state = {
            "domain": "nutrition",
            "source_schema": {},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "enable_enrichment": True,
        }

        with patch("src.agents.graph.BlockRegistry") as mock_reg, \
             patch("src.agents.graph.call_llm_json", return_value={"block_sequence": llm_sequence, "reasoning": "r"}), \
             patch("src.agents.graph.get_orchestrator_llm", return_value="m"), \
             patch("src.agents.graph.get_unified_schema") as mock_us:
            mock_us.return_value = MagicMock()
            inst = mock_reg.instance.return_value
            inst.get_default_sequence.return_value = pool
            inst.get_blocks_with_metadata.return_value = [{"name": b} for b in pool]
            inst.is_stage.return_value = False

            result = plan_sequence_node(state)

        sequence = result["block_sequence"]
        # All pool blocks must be present
        for b in pool:
            assert b in sequence
        # llm_enrich must be inserted BEFORE dq_score_post
        assert sequence.index("llm_enrich") < sequence.index("dq_score_post")

    def test_dropped_block_appended_at_end_when_no_dq_score_post(self):
        pool = ["a", "b", "c"]
        llm_sequence = ["a", "b"]
        state = {
            "domain": "x",
            "source_schema": {},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "enable_enrichment": True,
        }
        with patch("src.agents.graph.BlockRegistry") as mock_reg, \
             patch("src.agents.graph.call_llm_json", return_value={"block_sequence": llm_sequence, "reasoning": ""}), \
             patch("src.agents.graph.get_orchestrator_llm", return_value="m"), \
             patch("src.agents.graph.get_unified_schema") as mock_us:
            mock_us.return_value = MagicMock()
            inst = mock_reg.instance.return_value
            inst.get_default_sequence.return_value = pool
            inst.get_blocks_with_metadata.return_value = [{"name": b} for b in pool]
            inst.is_stage.return_value = False

            result = plan_sequence_node(state)

        assert result["block_sequence"][-1] == "c"

    def test_full_sequence_returned_when_llm_keeps_all_blocks(self):
        pool = ["dq_score_pre", "x", "dq_score_post"]
        state = {
            "domain": "x",
            "source_schema": {},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "enable_enrichment": True,
        }
        with patch("src.agents.graph.BlockRegistry") as mock_reg, \
             patch("src.agents.graph.call_llm_json", return_value={"block_sequence": pool, "reasoning": "ok"}), \
             patch("src.agents.graph.get_orchestrator_llm", return_value="m"), \
             patch("src.agents.graph.get_unified_schema") as mock_us:
            mock_us.return_value = MagicMock()
            inst = mock_reg.instance.return_value
            inst.get_default_sequence.return_value = pool
            inst.get_blocks_with_metadata.return_value = [{"name": b} for b in pool]
            inst.is_stage.return_value = False

            result = plan_sequence_node(state)
        assert result["block_sequence"] == pool

    def test_yaml_cache_written_when_fingerprint_present(self):
        pool = ["dq_score_pre", "dq_score_post"]
        cache_client = MagicMock()
        state = {
            "domain": "x",
            "source_schema": {},
            "gaps": [],
            "registry_misses": [],
            "block_registry_hits": {},
            "enable_enrichment": True,
            "_schema_fingerprint": "abc1234567890def",
            "cache_client": cache_client,
            "column_mapping": {"a": "b"},
            "operations": [{"primitive": "RENAME"}],
            "mapping_yaml_path": None,
        }
        with patch("src.agents.graph.BlockRegistry") as mock_reg, \
             patch("src.agents.graph.call_llm_json", return_value={"block_sequence": pool, "reasoning": "r"}), \
             patch("src.agents.graph.get_orchestrator_llm", return_value="m"), \
             patch("src.agents.graph.get_unified_schema") as mock_us:
            mock_us.return_value = MagicMock()
            inst = mock_reg.instance.return_value
            inst.get_default_sequence.return_value = pool
            inst.get_blocks_with_metadata.return_value = [{"name": b} for b in pool]
            inst.is_stage.return_value = False
            plan_sequence_node(state)

        cache_client.set.assert_called_once()
        args, kwargs = cache_client.set.call_args
        assert args[0] == "yaml"
        assert args[1] == "abc1234567890def"
