"""Unit tests for simple row-level block transforms."""

from __future__ import annotations

import pandas as pd
import pytest

from src.blocks.strip_whitespace import StripWhitespaceBlock
from src.blocks.strip_punctuation import StripPunctuationBlock
from src.blocks.lowercase_brand import LowercaseBrandBlock
from src.blocks.remove_noise_words import RemoveNoiseWordsBlock
from src.blocks.keep_quantity_in_name import KeepQuantityInNameBlock
from src.blocks.extract_allergens import ExtractAllergensBlock
from src.blocks.column_wise_merge import ColumnWiseMergeBlock


class TestStripWhitespace:
    def test_strips_leading_trailing(self):
        df = pd.DataFrame({"a": ["  hi ", " world"]})
        out = StripWhitespaceBlock().run(df)
        assert out["a"].tolist() == ["hi", "world"]

    def test_empty_becomes_na(self):
        df = pd.DataFrame({"a": ["   "]})
        out = StripWhitespaceBlock().run(df)
        assert pd.isna(out["a"].iloc[0])

    def test_non_string_column_ignored(self):
        df = pd.DataFrame({"n": [1, 2, 3], "s": [" a", "b ", "c"]})
        out = StripWhitespaceBlock().run(df)
        assert out["n"].tolist() == [1, 2, 3]

    def test_preserves_original_df(self):
        df = pd.DataFrame({"a": [" x "]})
        StripWhitespaceBlock().run(df)
        assert df["a"].iloc[0] == " x "


class TestStripPunctuation:
    def test_strips_punctuation_in_product_name(self):
        df = pd.DataFrame({"product_name": ["foo!@#bar", "hello, world."]})
        out = StripPunctuationBlock().run(df)
        assert out["product_name"].iloc[0] == "foo bar"
        assert out["product_name"].iloc[1] == "hello world"

    def test_brand_name_also_cleaned(self):
        df = pd.DataFrame({"brand_name": ["A&B Co."]})
        out = StripPunctuationBlock().run(df)
        assert "&" not in out["brand_name"].iloc[0]

    def test_na_preserved(self):
        df = pd.DataFrame({"product_name": [None, "x"]})
        out = StripPunctuationBlock().run(df)
        assert pd.isna(out["product_name"].iloc[0])

    def test_missing_columns_noop(self):
        df = pd.DataFrame({"other": ["x"]})
        out = StripPunctuationBlock().run(df)
        assert "other" in out.columns


class TestLowercaseBrand:
    def test_lowercases_brand(self):
        df = pd.DataFrame({"brand_name": ["KRAFT", "Nestle"]})
        out = LowercaseBrandBlock().run(df)
        assert out["brand_name"].tolist() == ["kraft", "nestle"]

    def test_missing_column_noop(self):
        df = pd.DataFrame({"x": [1]})
        out = LowercaseBrandBlock().run(df)
        assert out["x"].tolist() == [1]


class TestRemoveNoiseWords:
    def test_strips_legal_suffix(self):
        df = pd.DataFrame({"brand_name": ["Acme Inc.", "Foo LLC", "Bar Corp"]})
        out = RemoveNoiseWordsBlock().run(df)
        for v in out["brand_name"]:
            assert "Inc" not in v and "LLC" not in v and "Corp" not in v

    def test_na_preserved(self):
        df = pd.DataFrame({"brand_name": [None]})
        out = RemoveNoiseWordsBlock().run(df)
        assert pd.isna(out["brand_name"].iloc[0])

    def test_fallback_keeps_original_if_all_removed(self):
        df = pd.DataFrame({"brand_name": ["Inc"]})
        out = RemoveNoiseWordsBlock().run(df)
        # If clean output is empty, block falls back to original
        assert out["brand_name"].iloc[0] == "Inc"

    def test_missing_column_noop(self):
        df = pd.DataFrame({"x": [1]})
        out = RemoveNoiseWordsBlock().run(df)
        assert out["x"].tolist() == [1]


class TestKeepQuantityInName:
    def test_noop(self):
        df = pd.DataFrame({"product_name": ["milk 1L"]})
        out = KeepQuantityInNameBlock().run(df)
        assert out.equals(df)


class TestExtractAllergens:
    def test_detects_milk(self):
        df = pd.DataFrame({"ingredients": ["contains whole milk and sugar"]})
        out = ExtractAllergensBlock().run(df)
        assert "milk" in out["allergens"].iloc[0]

    def test_detects_multiple(self):
        df = pd.DataFrame({"ingredients": ["wheat flour with peanut"]})
        out = ExtractAllergensBlock().run(df)
        result = out["allergens"].iloc[0]
        assert "wheat" in result and "peanut" in result

    def test_empty_string_returns_empty(self):
        df = pd.DataFrame({"ingredients": ["apples bananas"]})
        out = ExtractAllergensBlock().run(df)
        assert out["allergens"].iloc[0] == ""

    def test_nan_ingredients_returns_none(self):
        df = pd.DataFrame({"ingredients": [None]})
        out = ExtractAllergensBlock().run(df)
        assert out["allergens"].iloc[0] is None

    def test_uses_recall_reason_fallback(self):
        df = pd.DataFrame({"recall_reason": ["undeclared peanut"]})
        out = ExtractAllergensBlock().run(df)
        assert "peanut" in out["allergens"].iloc[0]

    def test_no_ingredients_or_recall_columns(self):
        df = pd.DataFrame({"other": ["x"]})
        out = ExtractAllergensBlock().run(df)
        assert out["allergens"].isna().all()


class TestColumnWiseMerge:
    def test_no_group_id_noop(self):
        df = pd.DataFrame({"a": [1, 2]})
        out = ColumnWiseMergeBlock().run(df)
        assert out.equals(df)

    def test_merges_by_group(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1, 2],
            "name": ["short", "much longer name", "only"],
        })
        out = ColumnWiseMergeBlock().run(df)
        assert len(out) == 2
        # Longest string should win for group 1
        g1 = out[out["duplicate_group_id"] == 1]["name"].iloc[0]
        assert g1 == "much longer name"

    def test_empty_series_returns_na(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1],
            "val": [None, None],
        })
        out = ColumnWiseMergeBlock().run(df)
        assert pd.isna(out["val"].iloc[0])
