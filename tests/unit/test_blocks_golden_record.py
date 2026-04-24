"""Unit tests for GoldenRecordSelectBlock."""

from __future__ import annotations

import pandas as pd

from src.blocks.golden_record_select import GoldenRecordSelectBlock


class TestGoldenRecord:
    def test_no_group_id_noop(self):
        df = pd.DataFrame({"a": [1]})
        out = GoldenRecordSelectBlock().run(df)
        assert out.equals(df)

    def test_selects_most_complete(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1],
            "product_name": ["short", "much longer name here"],
            "brand_name": [None, "brand"],
            "ingredients": ["", "milk sugar flour eggs"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert len(out) == 1
        assert out["product_name"].iloc[0] == "much longer name here"

    def test_freshness_with_dates(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1],
            "published_date": ["2020-01-01", "2024-01-01"],
            "ingredients": ["same", "same"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert len(out) == 1

    def test_cleanup_temp_columns(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 2],
            "product_name": ["a", "b"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert "_completeness" not in out.columns
        assert "_golden_score" not in out.columns

    def test_multiple_groups(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1, 2, 2],
            "product_name": ["a", "aa", "b", "bb"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert len(out) == 2

    def test_no_date_column_default(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1],
            "product_name": ["aaa", "bbb"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert len(out) == 1

    def test_single_date_all_same(self):
        df = pd.DataFrame({
            "duplicate_group_id": [1, 1],
            "published_date": ["2024-01-01", "2024-01-01"],
            "product_name": ["a", "bb"],
        })
        out = GoldenRecordSelectBlock().run(df)
        assert len(out) == 1
