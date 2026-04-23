"""Tests for src/blocks/dq_score.py — pre/post DQ scoring with decimal precision."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from src.blocks.dq_score import (
    DQScorePostBlock,
    DQScorePreBlock,
    _SKIP_ALWAYS,
    compute_dq_score,
)


# ---------------------------------------------------------------------------
# compute_dq_score
# ---------------------------------------------------------------------------


class TestComputeDqScore:
    def test_score_in_valid_range(self, sample_dataframe):
        scores = compute_dq_score(sample_dataframe)
        assert (scores >= 0.0).all()
        assert (scores <= 100.0).all()

    def test_score_rounded_to_two_decimals(self, sample_dataframe):
        scores = compute_dq_score(sample_dataframe)
        # round(2) should produce values whose third decimal is 0
        # Multiplying by 100 and checking integer-ness verifies precision.
        for s in scores:
            assert round(s, 2) == s
            scaled = s * 100
            assert abs(scaled - round(scaled)) < 1e-6

    def test_score_returns_series_indexed_by_input(self, sample_dataframe):
        scores = compute_dq_score(sample_dataframe)
        assert isinstance(scores, pd.Series)
        assert list(scores.index) == list(sample_dataframe.index)

    def test_completeness_at_100_percent_no_freshness(self):
        df = pd.DataFrame({"product_name": ["a", "b"], "brand_name": ["x", "y"]})
        # No published_date → freshness defaults to 0.5
        # No ingredients → richness 0
        # completeness = 1.0
        scores = compute_dq_score(df)
        # 1.0*0.4 + 0.5*0.35 + 0*0.25 = 0.575 → 57.5%
        assert all(abs(s - 57.5) < 0.01 for s in scores)

    def test_completeness_at_zero_no_freshness(self):
        df = pd.DataFrame({"product_name": [None, None], "brand_name": [None, None]})
        scores = compute_dq_score(df)
        # 0.0*0.4 + 0.5*0.35 + 0*0.25 = 0.175 → 17.5%
        assert all(abs(s - 17.5) < 0.01 for s in scores)

    def test_skip_always_columns_excluded(self):
        df = pd.DataFrame(
            {
                "product_name": ["a", "b"],
                "dq_score_pre": [50.0, 50.0],
                "duplicate_group_id": [1, 2],
                "enriched_by_llm": [True, False],
            }
        )
        scores = compute_dq_score(df)
        # Only product_name counts → completeness = 1.0
        # 1.0 * 0.4 + 0.5 * 0.35 + 0 = 0.575
        assert all(abs(s - 57.5) < 0.01 for s in scores)

    def test_reference_columns_used_when_provided(self):
        # Even if more columns are added downstream, scoring uses the original set
        df_pre = pd.DataFrame({"product_name": ["a"], "brand_name": ["x"]})
        ref_cols = list(df_pre.columns)

        df_post = df_pre.copy()
        df_post["primary_category"] = ["Snacks"]
        df_post["allergens"] = [None]

        scores_post = compute_dq_score(df_post, reference_columns=ref_cols)
        # Completeness = 1.0 (both reference cols present)
        assert all(abs(s - 57.5) < 0.01 for s in scores_post)

    def test_custom_weights(self):
        df = pd.DataFrame({"product_name": ["a"]})
        weights = {"completeness": 1.0, "freshness": 0.0, "ingredient_richness": 0.0}
        scores = compute_dq_score(df, weights=weights)
        # 1.0 * 1.0 = 1.0 → 100.0
        assert scores.iloc[0] == 100.0

    def test_freshness_recent_date_high_score(self):
        today = pd.Timestamp("today").normalize()
        df = pd.DataFrame(
            {
                "product_name": ["a"],
                "published_date": [today],
            }
        )
        scores = compute_dq_score(df)
        # completeness=1.0, freshness=1.0, richness=0
        # 0.4 + 0.35 + 0 = 0.75 → 75.0
        assert abs(scores.iloc[0] - 75.0) < 0.5

    def test_freshness_old_date_low_score(self):
        very_old = pd.Timestamp("2000-01-01")
        df = pd.DataFrame(
            {
                "product_name": ["a"],
                "published_date": [very_old],
            }
        )
        scores = compute_dq_score(df)
        # 0+ years → freshness clamped to 0
        # completeness=1.0, freshness=0, richness=0
        # 0.4 + 0 + 0 = 0.4 → 40.0
        assert abs(scores.iloc[0] - 40.0) < 0.5

    def test_ingredient_richness_normalizes_to_max(self):
        df = pd.DataFrame(
            {
                "product_name": ["a", "b", "c"],
                "ingredients": ["short", "medium length string", "x" * 100],
            }
        )
        scores = compute_dq_score(df)
        # Row 0 should have lower richness than row 2
        assert scores.iloc[2] >= scores.iloc[0]

    def test_no_data_columns_returns_zero_completeness(self):
        # Only excluded columns present
        df = pd.DataFrame({"dq_score_pre": [50.0]})
        scores = compute_dq_score(df)
        # 0 * 0.4 + 0.5 * 0.35 + 0 = 0.175 → 17.5
        assert abs(scores.iloc[0] - 17.5) < 0.01


# ---------------------------------------------------------------------------
# DQScorePreBlock
# ---------------------------------------------------------------------------


class TestDQScorePreBlock:
    def test_writes_dq_score_pre_column(self, sample_dataframe):
        block = DQScorePreBlock()
        out = block.run(sample_dataframe)
        assert "dq_score_pre" in out.columns

    def test_stores_reference_columns_attr(self, sample_dataframe):
        block = DQScorePreBlock()
        out = block.run(sample_dataframe)
        ref = out.attrs.get("dq_reference_columns")
        assert ref is not None
        assert "product_name" in ref
        assert "dq_score_pre" not in ref  # excluded from itself

    def test_decimal_precision_two_places(self, sample_dataframe):
        block = DQScorePreBlock()
        out = block.run(sample_dataframe)
        for s in out["dq_score_pre"]:
            assert round(s, 2) == s

    def test_does_not_mutate_input(self, sample_dataframe):
        original = sample_dataframe.copy()
        DQScorePreBlock().run(sample_dataframe)
        pd.testing.assert_frame_equal(sample_dataframe, original)


# ---------------------------------------------------------------------------
# DQScorePostBlock
# ---------------------------------------------------------------------------


class TestDQScorePostBlock:
    def test_writes_dq_score_post_and_delta(self, sample_dataframe):
        df = DQScorePreBlock().run(sample_dataframe)
        out = DQScorePostBlock().run(df)
        assert "dq_score_post" in out.columns
        assert "dq_delta" in out.columns

    def test_delta_decimal_precision_two_places(self, sample_dataframe):
        df = DQScorePreBlock().run(sample_dataframe)
        out = DQScorePostBlock().run(df)
        for delta in out["dq_delta"]:
            assert round(float(delta), 2) == float(delta)

    def test_post_uses_reference_columns_from_pre(self):
        # Pre captures the original column set; Post must compute over the same
        # set so the delta is meaningful even if enrichment added new columns.
        df = pd.DataFrame({"product_name": ["a", "b"], "brand_name": ["x", "y"]})
        df = DQScorePreBlock().run(df)
        # Add an enrichment column post-pre
        df["primary_category"] = ["Snacks", "Snacks"]
        out = DQScorePostBlock().run(df)
        # Delta should be 0 because reference cols were both fully populated already
        assert all(abs(d) < 0.01 for d in out["dq_delta"])

    def test_delta_positive_when_enrichment_fills_nulls(self):
        # Need an enrichment column tracked in the reference set so dq_score_post
        # sees it improve. Here, "ingredients" starts null in row 0 then is filled.
        df = pd.DataFrame(
            {
                "product_name": ["a", "b"],
                "ingredients": [None, "sugar, salt"],
            }
        )
        df = DQScorePreBlock().run(df)
        # Simulate enrichment filling the null
        df.loc[0, "ingredients"] = "wheat, milk"
        out = DQScorePostBlock().run(df)
        # Row 0 went from null → populated; delta must be > 0
        assert out["dq_delta"].iloc[0] > 0
        # Row 1 was already populated; delta should be ~0
        assert abs(out["dq_delta"].iloc[1]) < 0.01

    def test_does_not_mutate_input(self, sample_dataframe):
        df = DQScorePreBlock().run(sample_dataframe)
        original = df.copy()
        DQScorePostBlock().run(df)
        pd.testing.assert_frame_equal(df, original)


# ---------------------------------------------------------------------------
# Decimal precision edge cases
# ---------------------------------------------------------------------------


class TestDecimalPrecision:
    def test_rounding_handles_half_correctly(self):
        # Construct a frame whose computed score is exactly at a decimal boundary
        df = pd.DataFrame(
            {
                "product_name": ["a"] * 8,
                "ingredients": [None, None, None, None, None, "x", "x", "x"],
            }
        )
        # Use weights that exercise odd fractions
        weights = {"completeness": 0.333333, "freshness": 0.333333, "ingredient_richness": 0.333334}
        scores = compute_dq_score(df, weights=weights)
        for s in scores:
            assert round(s, 2) == s

    def test_score_never_exceeds_100(self):
        df = pd.DataFrame(
            {"product_name": ["a"] * 5, "ingredients": ["x" * 1000] * 5}
        )
        # Heavily weighted toward completeness + richness — but cap at 100 must hold
        weights = {"completeness": 1.0, "freshness": 0.0, "ingredient_richness": 0.0}
        scores = compute_dq_score(df, weights=weights)
        assert (scores <= 100.0).all()

    def test_score_never_negative(self):
        df = pd.DataFrame({"product_name": [None] * 3})
        scores = compute_dq_score(df)
        assert (scores >= 0.0).all()

    def test_decimal_serializable(self, sample_dataframe):
        # Output scores must be cleanly convertible to Decimal — no float artifacts
        scores = compute_dq_score(sample_dataframe)
        for s in scores:
            d = Decimal(str(s))
            assert d == Decimal(str(round(s, 2)))
