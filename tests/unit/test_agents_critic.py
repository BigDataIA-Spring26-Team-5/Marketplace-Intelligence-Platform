"""Unit tests for src.agents.critic.critique_schema_node."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.agents.critic import critique_schema_node


@pytest.fixture
def base_state():
    return {
        "operations": [
            {"primitive": "RENAME", "source_column": "a", "target_column": "b"}
        ],
        "source_schema": {
            "__meta__": {"rows": 100},
            "a": {"dtype": "object", "null_rate": 0.0, "unique_count": 50},
        },
        "column_mapping": {"a": "b"},
        "domain": "nutrition",
    }


class TestCritiqueGuards:
    def test_skips_when_revised_operations_present(self, base_state):
        base_state["revised_operations"] = [{"primitive": "RENAME"}]
        out = critique_schema_node(base_state)
        assert out == {}

    def test_skips_when_no_operations(self):
        out = critique_schema_node({"operations": []})
        assert out == {}

    def test_skips_when_operations_missing(self):
        out = critique_schema_node({})
        assert out == {}


class TestCritiqueCallsLLM:
    def test_returns_revised_and_notes(self, base_state):
        fake_result = {
            "revised_operations": [
                {"primitive": "CAST", "source_column": "a", "target_column": "b"}
            ],
            "critique_notes": [
                {"rule": "Rule4", "column": "b", "correction": "RENAME → CAST"}
            ],
        }
        with (
            patch("src.agents.critic.call_llm_json", return_value=fake_result) as mock_llm,
            patch("src.agents.critic.get_critic_llm", return_value="fake-model"),
            patch("src.agents.critic.get_domain_schema") as mock_schema,
        ):
            mock_schema.return_value.for_prompt.return_value = {}
            out = critique_schema_node(base_state)

        assert out["revised_operations"] == fake_result["revised_operations"]
        assert out["critique_notes"] == fake_result["critique_notes"]
        mock_llm.assert_called_once()

    def test_falls_back_to_original_when_llm_missing_key(self, base_state):
        with (
            patch("src.agents.critic.call_llm_json", return_value={}),
            patch("src.agents.critic.get_critic_llm", return_value="m"),
            patch("src.agents.critic.get_domain_schema") as mock_schema,
        ):
            mock_schema.return_value.for_prompt.return_value = {}
            out = critique_schema_node(base_state)
        assert out["revised_operations"] == base_state["operations"]
        assert out["critique_notes"] == []

    def test_handles_empty_critique_notes(self, base_state):
        fake_result = {"revised_operations": base_state["operations"], "critique_notes": []}
        with (
            patch("src.agents.critic.call_llm_json", return_value=fake_result),
            patch("src.agents.critic.get_critic_llm", return_value="m"),
            patch("src.agents.critic.get_domain_schema") as mock_schema,
        ):
            mock_schema.return_value.for_prompt.return_value = {}
            out = critique_schema_node(base_state)
        assert out["critique_notes"] == []

    def test_domain_defaults_to_nutrition(self):
        state = {
            "operations": [{"primitive": "RENAME"}],
            "source_schema": {"__meta__": {}, "a": {}},
            "column_mapping": {},
        }
        with (
            patch("src.agents.critic.call_llm_json", return_value={"revised_operations": [], "critique_notes": []}),
            patch("src.agents.critic.get_critic_llm", return_value="m"),
            patch("src.agents.critic.get_domain_schema") as mock_schema,
        ):
            mock_schema.return_value.for_prompt.return_value = {}
            critique_schema_node(state)
            mock_schema.assert_called_once_with("nutrition")

    def test_logs_correction_target_column_alt_key(self, base_state):
        # critique_notes with 'target_column' instead of 'column'
        fake_result = {
            "revised_operations": base_state["operations"],
            "critique_notes": [
                {"rule": "R", "target_column": "b", "correction": "ok"}
            ],
        }
        with (
            patch("src.agents.critic.call_llm_json", return_value=fake_result),
            patch("src.agents.critic.get_critic_llm", return_value="m"),
            patch("src.agents.critic.get_domain_schema") as mock_schema,
        ):
            mock_schema.return_value.for_prompt.return_value = {}
            out = critique_schema_node(base_state)
        assert out["critique_notes"][0]["target_column"] == "b"
