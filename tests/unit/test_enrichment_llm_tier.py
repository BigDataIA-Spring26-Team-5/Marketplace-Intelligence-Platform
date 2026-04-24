"""Unit tests for src.enrichment.llm_tier."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.enrichment.llm_tier import (
    _safe_text,
    _compute_content_hash,
    _build_rag_prompt,
    _build_batch_rag_prompt,
    _call_one_batch,
    llm_enrich,
)


# ---------------------------------------------------------------------------
# _safe_text
# ---------------------------------------------------------------------------

class TestSafeText:
    def test_string(self):
        assert _safe_text("hello") == "hello"

    def test_none(self):
        assert _safe_text(None) == ""

    def test_nan(self):
        assert _safe_text(float("nan")) == ""

    def test_pandas_na(self):
        assert _safe_text(pd.NA) == ""

    def test_int(self):
        assert _safe_text(42) == "42"

    def test_unhashable_non_scalar(self):
        # pd.isna raises on list — fallback to str()
        out = _safe_text([1, 2])
        assert out == "[1, 2]"


# ---------------------------------------------------------------------------
# _compute_content_hash
# ---------------------------------------------------------------------------

class TestComputeContentHash:
    def test_deterministic(self):
        h1 = _compute_content_hash("prod", "desc", ["primary_category"])
        h2 = _compute_content_hash("prod", "desc", ["primary_category"])
        assert h1 == h2

    def test_length_is_16(self):
        assert len(_compute_content_hash("x", "y", ["a"])) == 16

    def test_different_name_different_hash(self):
        h1 = _compute_content_hash("a", "d", ["c"])
        h2 = _compute_content_hash("b", "d", ["c"])
        assert h1 != h2

    def test_column_order_invariant(self):
        h1 = _compute_content_hash("n", "d", ["a", "b"])
        h2 = _compute_content_hash("n", "d", ["b", "a"])
        assert h1 == h2

    def test_case_and_whitespace_name(self):
        h1 = _compute_content_hash("FooBar", "d", ["c"])
        h2 = _compute_content_hash("  foobar  ", "d", ["c"])
        assert h1 == h2


# ---------------------------------------------------------------------------
# _build_rag_prompt
# ---------------------------------------------------------------------------

class TestBuildRagPrompt:
    def test_includes_neighbors_when_present(self):
        row = pd.Series({"product_name": "Yogurt", "brand_name": "Acme"})
        neighbors = [{"product_name": "Greek Yogurt", "category": "Dairy", "similarity": 0.9}]
        prompt = _build_rag_prompt(row, neighbors)
        assert "Greek Yogurt" in prompt
        assert "Dairy" in prompt
        assert "0.90" in prompt or "0.9" in prompt

    def test_no_neighbors(self):
        row = pd.Series({"product_name": "Yogurt"})
        prompt = _build_rag_prompt(row, [])
        assert "Yogurt" in prompt

    def test_skips_missing_fields(self):
        row = pd.Series({"product_name": "X", "brand_name": None})
        prompt = _build_rag_prompt(row, [])
        assert "Brand" not in prompt

    def test_asks_for_primary_category(self):
        row = pd.Series({"product_name": "X"})
        prompt = _build_rag_prompt(row, [])
        assert "primary_category" in prompt


# ---------------------------------------------------------------------------
# _build_batch_rag_prompt
# ---------------------------------------------------------------------------

class TestBuildBatchRagPrompt:
    def test_multiple_rows(self):
        rows = [pd.Series({"product_name": "A"}), pd.Series({"product_name": "B"})]
        neighbors = [[], []]
        prompt = _build_batch_rag_prompt(rows, neighbors)
        assert "[0]" in prompt
        assert "[1]" in prompt

    def test_truncates_long_values(self):
        long_val = "x" * 500
        rows = [pd.Series({"product_name": long_val})]
        prompt = _build_batch_rag_prompt(rows, [[]])
        # Name is truncated to 200 chars
        assert len(prompt) < 500

    def test_includes_neighbors(self):
        rows = [pd.Series({"product_name": "A"})]
        neighbors = [[{"product_name": "Similar", "category": "Dairy", "similarity": 0.8}]]
        prompt = _build_batch_rag_prompt(rows, neighbors)
        assert "Similar" in prompt
        assert "Dairy" in prompt


# ---------------------------------------------------------------------------
# _call_one_batch
# ---------------------------------------------------------------------------

class TestCallOneBatch:
    def test_success_first_try(self):
        rl = MagicMock()

        async def fake_acquire():
            return None

        rl.acquire = fake_acquire

        async def fake_llm(**kwargs):
            return {"results": [{"idx": 0, "primary_category": "Dairy"}]}

        with patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=fake_llm):
            result = asyncio.run(_call_one_batch(
                miss_rows=[pd.Series({"product_name": "X"})],
                batch_neighbors=[[]],
                model="m",
                rate_limiter=rl,
                batch_label="b",
            ))
        assert result == {"results": [{"idx": 0, "primary_category": "Dairy"}]}

    def test_non_rate_limit_exception_returns_exception(self):
        rl = MagicMock()

        async def fake_acquire():
            return None

        rl.acquire = fake_acquire

        async def bad_llm(**kwargs):
            raise ValueError("bad")

        with patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=bad_llm):
            result = asyncio.run(_call_one_batch(
                miss_rows=[pd.Series({"product_name": "X"})],
                batch_neighbors=[[]],
                model="m",
                rate_limiter=rl,
                batch_label="b",
                max_retries=2,
            ))
        assert isinstance(result, Exception)

    def test_rate_limit_retries(self):
        rl = MagicMock()

        async def fake_acquire():
            return None

        async def fake_backoff(attempt):
            return None

        rl.acquire = fake_acquire
        rl.backoff = fake_backoff

        call_count = {"n": 0}

        async def maybe_fail(**kwargs):
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise Exception("429 rate limit")
            return {"results": []}

        with patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=maybe_fail):
            result = asyncio.run(_call_one_batch(
                miss_rows=[pd.Series({"product_name": "X"})],
                batch_neighbors=[[]],
                model="m",
                rate_limiter=rl,
                batch_label="b",
                max_retries=3,
            ))
        assert result == {"results": []}
        assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# llm_enrich
# ---------------------------------------------------------------------------

class TestLLMEnrich:
    def test_primary_category_not_in_enrich_cols(self):
        df = pd.DataFrame({"primary_category": [None]})
        mask = pd.Series([True])
        out_df, out_mask, stats = llm_enrich(df, ["allergens"], mask)
        assert stats == {"resolved": 0}

    def test_no_rows_need_enrichment(self):
        df = pd.DataFrame({"primary_category": ["Dairy"]})
        mask = pd.Series([False])
        out_df, out_mask, stats = llm_enrich(df, ["primary_category"], mask)
        assert stats == {"resolved": 0}

    def test_cache_hit_resolves_without_llm(self):
        df = pd.DataFrame({
            "product_name": ["Milk"],
            "primary_category": [None],
            "ingredients": ["whole milk"],
        })
        mask = pd.Series([True])

        cache_client = MagicMock()
        cache_client.get.return_value = json.dumps({"primary_category": "Dairy"}).encode()

        with (
            patch("src.enrichment.llm_tier.load_corpus", return_value=(None, [])),
            patch("src.enrichment.llm_tier.RateLimiter") if False else patch("src.enrichment.rate_limiter.RateLimiter"),
            patch("src.enrichment.llm_tier.get_enrichment_llm", return_value="m"),
        ):
            out_df, out_mask, stats = llm_enrich(
                df, ["primary_category"], mask, cache_client=cache_client,
            )

        assert stats["resolved"] == 1
        assert out_df.loc[0, "primary_category"] == "Dairy"

    def test_llm_batch_resolves(self):
        df = pd.DataFrame({
            "product_name": ["Unknown Thing"],
            "primary_category": [None],
            "ingredients": ["some stuff"],
            "_knn_neighbors": [None],
        })
        mask = pd.Series([True])

        async def fake_acquire(tokens=None):
            return None

        async def fake_backoff(attempt):
            return None

        fake_rl = MagicMock()
        fake_rl.acquire = fake_acquire
        fake_rl.backoff = fake_backoff
        fake_rl.min_interval = 0.01

        async def fake_async_llm(**kwargs):
            return {"results": [{"idx": 0, "primary_category": "Snacks"}]}

        with (
            patch("src.enrichment.llm_tier.load_corpus", return_value=(None, [])),
            patch("src.enrichment.rate_limiter.RateLimiter", return_value=fake_rl),
            patch("src.enrichment.llm_tier.get_enrichment_llm", return_value="m"),
            patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=fake_async_llm),
        ):
            out_df, out_mask, stats = llm_enrich(df, ["primary_category"], mask)

        assert stats["resolved"] == 1
        assert out_df.loc[0, "primary_category"] == "Snacks"

    def test_llm_exception_does_not_crash(self):
        df = pd.DataFrame({
            "product_name": ["X"],
            "primary_category": [None],
            "ingredients": ["y"],
            "_knn_neighbors": [None],
        })
        mask = pd.Series([True])

        async def fake_acquire(tokens=None):
            return None

        async def fake_backoff(attempt):
            return None

        fake_rl = MagicMock()
        fake_rl.acquire = fake_acquire
        fake_rl.backoff = fake_backoff
        fake_rl.min_interval = 0.01

        async def crash(**kwargs):
            raise ValueError("boom")

        with (
            patch("src.enrichment.llm_tier.load_corpus", return_value=(None, [])),
            patch("src.enrichment.rate_limiter.RateLimiter", return_value=fake_rl),
            patch("src.enrichment.llm_tier.get_enrichment_llm", return_value="m"),
            patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=crash),
        ):
            out_df, out_mask, stats = llm_enrich(df, ["primary_category"], mask)

        # Exception in LLM batch is caught, no rows resolved
        assert stats["resolved"] == 0

    def test_invalid_idx_skipped(self):
        df = pd.DataFrame({
            "product_name": ["X"],
            "primary_category": [None],
            "ingredients": ["y"],
            "_knn_neighbors": [None],
        })
        mask = pd.Series([True])

        async def fake_acquire(tokens=None):
            return None

        async def fake_backoff(attempt):
            return None

        fake_rl = MagicMock()
        fake_rl.acquire = fake_acquire
        fake_rl.backoff = fake_backoff
        fake_rl.min_interval = 0.01

        async def fake_async_llm(**kwargs):
            return {"results": [
                {"idx": 99, "primary_category": "Dairy"},  # out of bounds
                {"idx": None, "primary_category": "Snacks"},  # invalid
                {"primary_category": "Other"},  # missing idx
            ]}

        with (
            patch("src.enrichment.llm_tier.load_corpus", return_value=(None, [])),
            patch("src.enrichment.rate_limiter.RateLimiter", return_value=fake_rl),
            patch("src.enrichment.llm_tier.get_enrichment_llm", return_value="m"),
            patch("src.enrichment.llm_tier.async_call_llm_json", side_effect=fake_async_llm),
        ):
            out_df, _, stats = llm_enrich(df, ["primary_category"], mask)

        assert stats["resolved"] == 0
