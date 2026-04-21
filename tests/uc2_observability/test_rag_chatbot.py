"""Tests for ObservabilityChatbot."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.uc2_observability.log_store import RunLogStore
from src.uc2_observability.rag_chatbot import ChatResponse, ObservabilityChatbot


def _make_log(run_id: str, timestamp: str, source_name: str = "usda",
              status: str = "success", dq_delta: float = 0.1) -> dict:
    return {
        "run_id": run_id,
        "timestamp": timestamp,
        "source_name": source_name,
        "status": status,
        "dq_delta": dq_delta,
    }


def _store_with_logs(logs: list[dict]) -> RunLogStore:
    store = MagicMock(spec=RunLogStore)
    store.load_all.return_value = logs
    return store


_FIXTURE_LOGS = [
    _make_log("aaaaaaaa-0000-0000-0000-000000000001", "2026-04-20T10:00:00", source_name="usda"),
    _make_log("bbbbbbbb-0000-0000-0000-000000000002", "2026-04-21T10:00:00", source_name="fda"),
    _make_log("cccccccc-0000-0000-0000-000000000003", "2026-04-22T10:00:00", source_name="usda", dq_delta=0.15),
    _make_log("dddddddd-0000-0000-0000-000000000004", "2026-04-23T10:00:00", source_name="fda", dq_delta=0.05),
    _make_log("eeeeeeee-0000-0000-0000-000000000005", "2026-04-24T10:00:00", source_name="usda", dq_delta=0.2),
]


class TestGetRelevantContext:
    def _bot(self) -> ObservabilityChatbot:
        bot = ObservabilityChatbot(_store_with_logs(_FIXTURE_LOGS))
        bot._logs = list(_FIXTURE_LOGS)
        return bot

    def test_run_id_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("Tell me about run aaaaaaaa-0000-0000-0000-000000000001")
        assert len(result) == 1
        assert result[0]["run_id"] == "aaaaaaaa-0000-0000-0000-000000000001"

    def test_source_name_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("What happened with fda runs?")
        assert all(r["source_name"] == "fda" for r in result)
        assert len(result) == 2

    def test_last_n_runs_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("Show me the last 3 runs")
        assert len(result) == 3
        # should be sorted desc (most recent first)
        assert result[0]["timestamp"] >= result[1]["timestamp"]

    def test_recency_time_words_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("What are the most recent runs?", max_runs=2)
        assert len(result) == 2

    def test_metric_keyword_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("Show dq delta trend", max_runs=3)
        assert len(result) == 3

    def test_default_branch(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("How are things going?", max_runs=4)
        assert len(result) == 4


class TestQuery:
    def test_returns_no_data_when_empty_store(self) -> None:
        bot = ObservabilityChatbot(_store_with_logs([]))
        with patch("src.models.llm.call_llm") as mock_llm:
            resp = bot.query("How many runs succeeded?")
        mock_llm.assert_not_called()
        assert isinstance(resp, ChatResponse)
        assert resp.cited_run_ids == []
        assert "no" in resp.answer.lower() or "not" in resp.answer.lower() or "available" in resp.answer.lower()

    def test_returns_cited_run_ids_from_fixture(self) -> None:
        bot = ObservabilityChatbot(_store_with_logs(_FIXTURE_LOGS))
        fake_answer = (
            "The run aaaaaaaa-0000-0000-0000-000000000001 had dq_delta=0.1 and "
            "run bbbbbbbb-0000-0000-0000-000000000002 had dq_delta=0.1."
        )
        with patch("src.models.llm.call_llm", return_value=fake_answer):
            resp = bot.query("What was the dq delta for each run?")
        assert isinstance(resp, ChatResponse)
        assert "aaaaaaaa-0000-0000-0000-000000000001" in resp.cited_run_ids
        assert "bbbbbbbb-0000-0000-0000-000000000002" in resp.cited_run_ids
        assert resp.context_run_count > 0

    def test_never_raises_on_llm_exception(self) -> None:
        bot = ObservabilityChatbot(_store_with_logs(_FIXTURE_LOGS))
        with patch("src.models.llm.call_llm", side_effect=RuntimeError("LLM down")):
            resp = bot.query("What happened?")
        assert isinstance(resp, ChatResponse)
        assert resp.cited_run_ids == []

    def test_cited_ids_only_from_context(self) -> None:
        bot = ObservabilityChatbot(_store_with_logs(_FIXTURE_LOGS))
        hallucinated_id = "ffffffff-9999-9999-9999-999999999999"
        fake_answer = f"Run {hallucinated_id} was amazing and run aaaaaaaa-0000-0000-0000-000000000001 was good."
        with patch("src.models.llm.call_llm", return_value=fake_answer):
            resp = bot.query("Tell me about runs")
        assert hallucinated_id not in resp.cited_run_ids


class TestUS4Extensions:
    def _bot(self) -> ObservabilityChatbot:
        bot = ObservabilityChatbot(_store_with_logs(_FIXTURE_LOGS))
        bot._logs = list(_FIXTURE_LOGS)
        return bot

    def test_last_5_runs_returns_5(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("Show me the last 5 runs")
        assert len(result) == 5

    def test_source_grouped_comparison(self) -> None:
        bot = self._bot()
        result = bot.get_relevant_context("Compare usda and fda enrichment rates", max_runs=2)
        sources = {r["source_name"] for r in result}
        assert "usda" in sources
        assert "fda" in sources
