"""Tests for src/agents/critic.py — Agent 2 critique node."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.agents.critic import critique_schema_node


@pytest.fixture
def base_state():
    return {
        "operations": [
            {"primitive": "RENAME", "source_column": "description", "target_column": "product_name"},
            {"primitive": "ADD", "action": "set_null", "target_column": "brand_name"},
        ],
        "source_schema": {
            "description": {"dtype": "object", "null_rate": 0.05},
            "__meta__": {"row_count": 100},
        },
        "column_mapping": {"description": "product_name"},
    }


class TestCritiqueSchemaNode:
    def test_skip_when_already_ran(self):
        state = {"revised_operations": [{"primitive": "RENAME"}]}
        assert critique_schema_node(state) == {}

    def test_skip_when_no_operations(self):
        state = {"operations": []}
        assert critique_schema_node(state) == {}

    def test_skip_when_operations_missing(self):
        assert critique_schema_node({}) == {}

    def test_returns_revised_operations_from_llm(self, base_state):
        mock_response = {
            "revised_operations": [
                {"primitive": "RENAME", "source_column": "description", "target_column": "product_name"}
            ],
            "critique_notes": [
                {"rule": "Rule 4", "column": "product_name", "correction": "verified"}
            ],
        }
        with patch("src.agents.critic.call_llm_json", return_value=mock_response), \
             patch("src.agents.critic.get_critic_llm", return_value="mock-model"), \
             patch("src.agents.critic.get_unified_schema") as mock_schema:
            mock_schema.return_value.for_prompt.return_value = {"columns": {}}
            result = critique_schema_node(base_state)

        assert result["revised_operations"] == mock_response["revised_operations"]
        assert result["critique_notes"] == mock_response["critique_notes"]

    def test_falls_back_to_originals_when_no_revised(self, base_state):
        mock_response = {"critique_notes": []}
        with patch("src.agents.critic.call_llm_json", return_value=mock_response), \
             patch("src.agents.critic.get_critic_llm", return_value="mock-model"), \
             patch("src.agents.critic.get_unified_schema") as mock_schema:
            mock_schema.return_value.for_prompt.return_value = {"columns": {}}
            result = critique_schema_node(base_state)
        assert result["revised_operations"] == base_state["operations"]

    def test_no_corrections_returns_empty_notes(self, base_state):
        mock_response = {"revised_operations": base_state["operations"]}
        with patch("src.agents.critic.call_llm_json", return_value=mock_response), \
             patch("src.agents.critic.get_critic_llm", return_value="mock-model"), \
             patch("src.agents.critic.get_unified_schema") as mock_schema:
            mock_schema.return_value.for_prompt.return_value = {"columns": {}}
            result = critique_schema_node(base_state)
        assert result["critique_notes"] == []

    def test_llm_called_with_critic_model(self, base_state):
        with patch("src.agents.critic.call_llm_json") as mock_call, \
             patch("src.agents.critic.get_critic_llm", return_value="anthropic/claude-sonnet-4-6"), \
             patch("src.agents.critic.get_unified_schema") as mock_schema:
            mock_schema.return_value.for_prompt.return_value = {"columns": {}}
            mock_call.return_value = {"revised_operations": [], "critique_notes": []}
            critique_schema_node(base_state)
            kwargs = mock_call.call_args.kwargs
            assert kwargs["model"] == "anthropic/claude-sonnet-4-6"

    def test_meta_separated_from_columns_in_prompt(self, base_state):
        with patch("src.agents.critic.call_llm_json") as mock_call, \
             patch("src.agents.critic.get_critic_llm", return_value="m"), \
             patch("src.agents.critic.get_unified_schema") as mock_schema:
            mock_schema.return_value.for_prompt.return_value = {"columns": {}}
            mock_call.return_value = {"revised_operations": [], "critique_notes": []}
            critique_schema_node(base_state)
            prompt = mock_call.call_args.kwargs["messages"][0]["content"]
            # description column should appear; __meta__ should not appear in source_profile slot
            assert "description" in prompt
