"""Unit tests for deterministic enrichment (Tier 1)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.enrichment.deterministic import (
    CATEGORY_RULES,
    DIETARY_RULES,
    ORGANIC_PATTERN,
    deterministic_enrich,
)


def _df(products, ingredients=None):
    return pd.DataFrame({
        "product_name": products,
        "ingredients": ingredients or [""] * len(products),
        "primary_category": [None] * len(products),
        "dietary_tags": [None] * len(products),
        "is_organic": [None] * len(products),
    })


class TestCategory:
    def test_dairy(self):
        df = _df(["Whole Milk 1 gal"])
        mask = pd.Series([True])
        out, _, stats = deterministic_enrich(df, ["primary_category"], mask)
        assert out.at[0, "primary_category"] == "Dairy"
        assert stats["resolved"] == 1

    def test_beverage(self):
        df = _df(["Orange Juice"])
        out, _, _ = deterministic_enrich(df, ["primary_category"], pd.Series([True]))
        assert out.at[0, "primary_category"] == "Beverages"

    def test_meat(self):
        df = _df(["Smoked Bacon"])
        out, _, _ = deterministic_enrich(df, ["primary_category"], pd.Series([True]))
        assert out.at[0, "primary_category"] == "Meat & Poultry"

    def test_seafood(self):
        df = _df(["Canned Tuna"])
        out, _, _ = deterministic_enrich(df, ["primary_category"], pd.Series([True]))
        assert out.at[0, "primary_category"] == "Seafood"

    def test_baby(self):
        df = _df(["Infant Formula"])
        out, _, _ = deterministic_enrich(df, ["primary_category"], pd.Series([True]))
        assert out.at[0, "primary_category"] == "Baby Food"

    def test_no_match_stays_none(self):
        df = _df(["xyzzzzzzzz"])
        out, _, stats = deterministic_enrich(df, ["primary_category"], pd.Series([True]))
        assert pd.isna(out.at[0, "primary_category"])


class TestDietary:
    def test_vegan(self):
        df = _df(["Vegan Cheese Spread"])
        out, _, _ = deterministic_enrich(df, ["dietary_tags"], pd.Series([True]))
        assert "vegan" in out.at[0, "dietary_tags"]

    def test_gluten_free_with_hyphen(self):
        df = _df(["Gluten-Free Pasta"])
        out, _, _ = deterministic_enrich(df, ["dietary_tags"], pd.Series([True]))
        assert "gluten-free" in out.at[0, "dietary_tags"]

    def test_multiple_tags(self):
        df = _df(["Kosher Vegan Snack"])
        out, _, _ = deterministic_enrich(df, ["dietary_tags"], pd.Series([True]))
        tags = out.at[0, "dietary_tags"]
        assert "kosher" in tags and "vegan" in tags

    def test_no_tags_empty_string(self):
        df = _df(["Plain Thing"])
        out, _, _ = deterministic_enrich(df, ["dietary_tags"], pd.Series([True]))
        assert out.at[0, "dietary_tags"] == ""


class TestIsOrganic:
    def test_organic_true(self):
        df = _df(["Organic Apples"])
        out, _, _ = deterministic_enrich(df, ["is_organic"], pd.Series([True]))
        assert out.at[0, "is_organic"] is True

    def test_not_organic_false(self):
        df = _df(["Regular Apples"])
        out, _, _ = deterministic_enrich(df, ["is_organic"], pd.Series([True]))
        assert out.at[0, "is_organic"] is False


class TestEdgeCases:
    def test_no_text_cols_no_op(self):
        df = pd.DataFrame({"other": [1]})
        mask = pd.Series([True])
        out, m, stats = deterministic_enrich(df, ["primary_category"], mask)
        assert stats["resolved"] == 0

    def test_mask_false_is_skipped(self):
        df = _df(["Whole Milk"])
        mask = pd.Series([False])
        out, _, _ = deterministic_enrich(df, ["primary_category"], mask)
        assert pd.isna(out.at[0, "primary_category"])


class TestRuleTables:
    def test_rules_are_nonempty(self):
        assert len(CATEGORY_RULES) > 10
        assert len(DIETARY_RULES) >= 5

    def test_organic_pattern_matches(self):
        assert ORGANIC_PATTERN.search("usda organic apples")
        assert ORGANIC_PATTERN.search("Organic")
