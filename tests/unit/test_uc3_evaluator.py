"""Unit tests for UC3 SearchEvaluator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.uc3_search.evaluator import SearchEvaluator, ESCI_LABEL_SCORES


@pytest.fixture
def esci_df():
    return pd.DataFrame([
        {"query": "oats", "product_title": "Organic Oats", "esci_label": "E"},
        {"query": "oats", "product_title": "Corn Flakes", "esci_label": "I"},
        {"query": "milk", "product_title": "Whole Milk", "esci_label": "S"},
    ])


class TestSampleQueries:
    def test_returns_queries(self, esci_df):
        ev = SearchEvaluator(esci_df)
        q = ev._sample_queries(10)
        assert "oats" in q and "milk" in q

    def test_limits_n(self, esci_df):
        ev = SearchEvaluator(esci_df)
        assert len(ev._sample_queries(1)) == 1

    def test_empty_df(self):
        ev = SearchEvaluator(pd.DataFrame())
        assert ev._sample_queries(5) == []

    def test_none_df(self):
        ev = SearchEvaluator(None)
        assert ev._sample_queries(5) == []


class TestGroundTruth:
    def test_exact_match(self, esci_df):
        ev = SearchEvaluator(esci_df)
        assert ev._ground_truth_label("oats", "Organic Oats") == 3

    def test_no_match(self, esci_df):
        ev = SearchEvaluator(esci_df)
        assert ev._ground_truth_label("oats", "Nonexistent") is None

    def test_none_df(self):
        ev = SearchEvaluator(None)
        assert ev._ground_truth_label("q", "p") is None


class TestJudgeRelevance:
    def test_uses_ground_truth(self, esci_df):
        ev = SearchEvaluator(esci_df)
        score = ev.judge_relevance("oats", {"product_name": "Organic Oats"})
        assert score == 3

    def test_falls_back_to_llm(self, esci_df):
        ev = SearchEvaluator(esci_df)
        with patch.object(ev, "_llm_judge", return_value="E"):
            score = ev.judge_relevance("xyz", {"product_name": "never"})
        assert score == 3


class TestNDCG:
    def test_empty(self):
        assert SearchEvaluator().compute_ndcg([]) == 0.0

    def test_perfect_order(self):
        ev = SearchEvaluator()
        assert ev.compute_ndcg([3, 2, 1, 0]) == 1.0

    def test_worse_than_ideal(self):
        ev = SearchEvaluator()
        val = ev.compute_ndcg([0, 3, 2])
        assert 0 < val < 1

    def test_all_zero(self):
        assert SearchEvaluator().compute_ndcg([0, 0, 0]) == 0.0


class TestMRR:
    def test_first_relevant(self):
        assert SearchEvaluator().compute_mrr([3, 0, 0]) == 1.0

    def test_second_relevant(self):
        assert SearchEvaluator().compute_mrr([0, 2, 0]) == 0.5

    def test_none_relevant(self):
        assert SearchEvaluator().compute_mrr([0, 1, 1]) == 0.0

    def test_empty(self):
        assert SearchEvaluator().compute_mrr([]) == 0.0


class TestAggregate:
    def test_computes_metrics_and_dist(self):
        ev = SearchEvaluator()
        out = ev._aggregate([[3, 2, 0], [3, 0, 0]], k=10)
        assert "ndcg" in out and "mrr" in out
        assert out["relevance_distribution"]["E"] == 2
        assert out["n_queries"] == 2

    def test_empty_scores(self):
        ev = SearchEvaluator()
        out = ev._aggregate([], k=10)
        assert out["ndcg"] == 0.0
        assert out["mrr"] == 0.0


class TestLLMJudge:
    def test_import_error_returns_irrelevant(self, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "anthropic", None)
        ev = SearchEvaluator()
        label = ev._llm_judge("q", {"product_name": "p"})
        assert label == "I"

    def test_successful_call(self):
        ev = SearchEvaluator()
        fake_anthropic = MagicMock()
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text="E")]
        client.messages.create.return_value = msg
        fake_anthropic.Anthropic.return_value = client
        with patch.dict("sys.modules", {"anthropic": fake_anthropic}):
            label = ev._llm_judge("q", {"product_name": "p"})
        assert label == "E"

    def test_invalid_label_defaults_to_I(self):
        ev = SearchEvaluator()
        fake_anthropic = MagicMock()
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text="ZZZ")]
        client.messages.create.return_value = msg
        fake_anthropic.Anthropic.return_value = client
        with patch.dict("sys.modules", {"anthropic": fake_anthropic}):
            label = ev._llm_judge("q", {"product_name": "p"})
        assert label == "I"


class TestRun:
    def test_full_run(self, esci_df):
        ev = SearchEvaluator(esci_df)

        def fake_search(q, k):
            return [{"product_name": "Organic Oats"}, {"product_name": "Corn Flakes"}]

        report = ev.run(fake_search, fake_search, n_queries=1, top_k=2)
        assert "before" in report and "after" in report
        assert "delta" in report
        assert report["n_queries"] == 1
