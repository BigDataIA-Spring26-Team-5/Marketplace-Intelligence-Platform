"""Unit tests for SchemaEnforceBlock."""

from __future__ import annotations

import pandas as pd
import pytest

from src.blocks.schema_enforce import SchemaEnforceBlock, _silver_columns_from_schema
from src.schema.models import UnifiedSchema, ColumnSpec


@pytest.fixture
def schema():
    return UnifiedSchema(columns={
        "product_name": ColumnSpec(type="string", required=True),
        "price": ColumnSpec(type="float"),
        "count": ColumnSpec(type="integer"),
        "is_organic": ColumnSpec(type="boolean"),
        "dq_score_pre": ColumnSpec(type="float", computed=True),
        "dq_score_post": ColumnSpec(type="float", computed=True),
        "dq_delta": ColumnSpec(type="float", computed=True),
    })


class TestSilverColumns:
    def test_excludes_gold_only(self, schema):
        cols = _silver_columns_from_schema(schema)
        names = [c for c, _ in cols]
        assert "dq_score_post" not in names
        assert "dq_delta" not in names
        assert "dq_score_pre" in names

    def test_dtype_mapping(self, schema):
        cols = dict(_silver_columns_from_schema(schema))
        assert cols["price"] == "Float64"
        assert cols["count"] == "Int64"
        assert cols["product_name"] == "string"
        assert cols["is_organic"] == "boolean"


class TestSchemaEnforceBlock:
    def test_missing_config_raises(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError, match="unified_schema"):
            SchemaEnforceBlock().run(df, config={})

    def test_drops_extra_columns(self, schema):
        df = pd.DataFrame({
            "product_name": ["a"],
            "price": [1.0],
            "count": [1],
            "is_organic": [True],
            "dq_score_pre": [0.5],
            "extra_column": ["should_go"],
        })
        out = SchemaEnforceBlock().run(df, config={"unified_schema": schema})
        assert "extra_column" not in out.columns

    def test_adds_missing_columns(self, schema):
        df = pd.DataFrame({"product_name": ["a"]})
        out = SchemaEnforceBlock().run(df, config={"unified_schema": schema})
        assert "price" in out.columns
        assert "count" in out.columns
        assert "dq_score_post" not in out.columns  # gold-only

    def test_casts_existing_columns(self, schema):
        df = pd.DataFrame({
            "product_name": ["a"],
            "price": ["1.5"],
            "count": ["3"],
            "is_organic": ["true"],
        })
        out = SchemaEnforceBlock().run(df, config={"unified_schema": schema})
        assert str(out["price"].dtype) == "Float64"

    def test_schema_order_preserved(self, schema):
        df = pd.DataFrame({
            "is_organic": [True],
            "product_name": ["x"],
        })
        out = SchemaEnforceBlock().run(df, config={"unified_schema": schema})
        cols = list(out.columns)
        assert cols.index("product_name") < cols.index("is_organic")

    def test_bad_cast_warns_but_continues(self, schema):
        df = pd.DataFrame({
            "product_name": ["a"],
            "price": ["not_a_number"],  # cannot cast to Float64
        })
        out = SchemaEnforceBlock().run(df, config={"unified_schema": schema})
        assert "price" in out.columns
