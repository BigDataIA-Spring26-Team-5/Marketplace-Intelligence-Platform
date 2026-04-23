"""Tests for src/agents/orchestrator.py — Agent 1 helpers + LLM response parsing."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from src.agents.confidence import (
    ConfidenceScore,
    calculate_confidence,
    get_confidence_display,
    get_confidence_level,
)
from src.agents.orchestrator import (
    _BLOCK_COLUMN_PROVIDERS,
    _DTYPE_FAMILY,
    _IDENTITY_COLUMNS,
    _compute_schema_fingerprint,
    _deterministic_corrections,
    _detect_enrichment_columns,
    _llm_op_to_yaml,
    _parse_llm_response,
    _to_snake,
)
from src.schema.models import ColumnSpec, UnifiedSchema


# ---------------------------------------------------------------------------
# _to_snake
# ---------------------------------------------------------------------------


class TestToSnake:
    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("camelCase", "camel_case"),
            ("PascalCase", "pascal_case"),
            ("HTTPRequest", "http_request"),
            ("getHTTPResponseCode", "get_http_response_code"),
            ("snake_case", "snake_case"),
            ("With Spaces", "with_spaces"),
            ("dash-separated", "dash_separated"),
            ("ALLCAPS", "allcaps"),
        ],
    )
    def test_normalizes_to_snake(self, input_name, expected):
        assert _to_snake(input_name) == expected


# ---------------------------------------------------------------------------
# _parse_llm_response
# ---------------------------------------------------------------------------


class TestParseLlmResponse:
    def test_new_format_with_operations(self):
        result = {
            "column_mapping": {"a": "b"},
            "operations": [{"primitive": "RENAME"}],
            "unresolvable": [{"target_column": "x"}],
        }
        cm, ops, unr, legacy = _parse_llm_response(result)
        assert cm == {"a": "b"}
        assert ops == [{"primitive": "RENAME"}]
        assert unr == [{"target_column": "x"}]
        assert legacy == []

    def test_legacy_format_derivable_gaps(self):
        result = {
            "column_mapping": {},
            "derivable_gaps": [{"target_column": "x", "action": "CAST"}],
            "missing_columns": [{"target_column": "y"}],
        }
        cm, ops, unr, legacy = _parse_llm_response(result)
        assert ops == []
        assert any(g["target_column"] == "x" for g in legacy)
        assert any(g["target_column"] == "y" and g["action"] == "MISSING" for g in legacy)

    def test_oldest_flat_gaps_format(self):
        result = {"column_mapping": {}, "gaps": [{"target_column": "x"}]}
        cm, ops, unr, legacy = _parse_llm_response(result)
        assert legacy == [{"target_column": "x"}]


# ---------------------------------------------------------------------------
# _detect_enrichment_columns
# ---------------------------------------------------------------------------


class TestDetectEnrichmentColumns:
    def test_returns_enrichment_cols_absent_from_source(self):
        unified = UnifiedSchema(
            columns={
                "product_name": ColumnSpec(type="string"),
                "primary_category": ColumnSpec(type="string", enrichment=True),
                "allergens": ColumnSpec(type="string", enrichment=True),
            }
        )
        source_schema = {"product_name": {"dtype": "object"}, "__meta__": {}}
        absent = _detect_enrichment_columns(unified, source_schema)
        assert "primary_category" in absent
        assert "allergens" in absent

    def test_excludes_enrichment_cols_present_in_source(self):
        unified = UnifiedSchema(
            columns={
                "primary_category": ColumnSpec(type="string", enrichment=True),
            }
        )
        source_schema = {"primary_category": {"dtype": "object"}, "__meta__": {}}
        assert _detect_enrichment_columns(unified, source_schema) == []


# ---------------------------------------------------------------------------
# _compute_schema_fingerprint
# ---------------------------------------------------------------------------


class TestSchemaFingerprint:
    def test_deterministic_across_column_order(self):
        s1 = {"a": {}, "b": {}, "c": {}, "__meta__": {}}
        s2 = {"c": {}, "a": {}, "b": {}, "__meta__": {}}
        fp1 = _compute_schema_fingerprint(s1, "nutrition", "1.0")
        fp2 = _compute_schema_fingerprint(s2, "nutrition", "1.0")
        assert fp1 == fp2

    def test_different_columns_give_different_fingerprint(self):
        fp1 = _compute_schema_fingerprint({"a": {}, "__meta__": {}}, "x", "1.0")
        fp2 = _compute_schema_fingerprint({"b": {}, "__meta__": {}}, "x", "1.0")
        assert fp1 != fp2

    def test_different_domain_gives_different_fingerprint(self):
        s = {"a": {}, "__meta__": {}}
        assert _compute_schema_fingerprint(s, "nutrition", "1.0") != _compute_schema_fingerprint(s, "safety", "1.0")

    def test_different_version_gives_different_fingerprint(self):
        s = {"a": {}, "__meta__": {}}
        assert _compute_schema_fingerprint(s, "x", "1.0") != _compute_schema_fingerprint(s, "x", "2.0")

    def test_fingerprint_is_16_hex_chars(self):
        fp = _compute_schema_fingerprint({"a": {}, "__meta__": {}}, "x", "1.0")
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)


# ---------------------------------------------------------------------------
# _deterministic_corrections (Rules 4, 6, 7)
# ---------------------------------------------------------------------------


class TestDeterministicCorrections:
    @pytest.fixture
    def schema(self):
        return UnifiedSchema(
            columns={
                "product_name": ColumnSpec(type="string"),
                "brand_name": ColumnSpec(type="string"),
                "price_usd": ColumnSpec(type="float"),
                "stock_qty": ColumnSpec(type="integer"),
            }
        )

    def test_rule4_string_to_float_rename_becomes_cast(self, schema):
        ops = [
            {
                "primitive": "RENAME",
                "source_column": "p",
                "target_column": "price_usd",
            }
        ]
        source = {"p": {"dtype": "object"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {"p": "price_usd"}, source, schema)
        assert out[0]["primitive"] == "CAST"
        assert out[0]["target_type"] == "float"

    def test_rule4_compatible_types_unchanged(self, schema):
        ops = [
            {
                "primitive": "RENAME",
                "source_column": "n",
                "target_column": "product_name",
            }
        ]
        source = {"n": {"dtype": "object"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {"n": "product_name"}, source, schema)
        assert out[0]["primitive"] == "RENAME"

    def test_rule6_uncovered_source_gets_delete(self, schema):
        # Column "extra" is in source but never consumed → DELETE injected
        ops = []
        source = {"extra": {"dtype": "object"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {}, source, schema)
        delete_ops = [op for op in out if op.get("primitive") == "DELETE"]
        assert any(op.get("source_column") == "extra" for op in delete_ops)

    def test_rule6_does_not_delete_protected_columns(self, schema):
        # Protected enrichment block columns must never be DELETE'd
        ops = [
            {"primitive": "DELETE", "source_column": "allergens"},
        ]
        source = {"allergens": {"dtype": "object"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {}, source, schema)
        assert not any(op.get("source_column") == "allergens" and op.get("primitive") == "DELETE" for op in out)

    def test_rule7_normalize_before_dedup_added_to_identity(self, schema):
        ops = [
            {"primitive": "RENAME", "source_column": "n", "target_column": "product_name"},
        ]
        source = {"n": {"dtype": "object"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {"n": "product_name"}, source, schema)
        prod_op = next(op for op in out if op.get("target_column") == "product_name")
        assert prod_op.get("normalize_before_dedup") is True

    def test_rule7_not_added_to_non_identity_columns(self, schema):
        ops = [
            {"primitive": "RENAME", "source_column": "p", "target_column": "price_usd"},
        ]
        source = {"p": {"dtype": "float64"}, "__meta__": {}}
        out = _deterministic_corrections(ops, {"p": "price_usd"}, source, schema)
        price_op = next(op for op in out if op.get("target_column") == "price_usd")
        assert "normalize_before_dedup" not in price_op


# ---------------------------------------------------------------------------
# _llm_op_to_yaml
# ---------------------------------------------------------------------------


class TestLlmOpToYaml:
    def test_add_set_null(self):
        op = {"primitive": "ADD", "target_column": "x", "target_type": "string", "reason": "missing"}
        out = _llm_op_to_yaml(op, {})
        assert out == {
            "target": "x",
            "type": "string",
            "action": "set_null",
            "status": "missing",
            "reason": "missing",
        }

    def test_add_set_default(self):
        op = {
            "primitive": "ADD",
            "action": "set_default",
            "target_column": "country",
            "target_type": "string",
            "default_value": "USA",
        }
        out = _llm_op_to_yaml(op, {})
        assert out["action"] == "set_default"
        assert out["default_value"] == "USA"

    def test_cast(self):
        op = {
            "primitive": "CAST",
            "source_column": "raw_price",
            "target_column": "price_usd",
            "target_type": "float",
            "source_type": "string",
        }
        out = _llm_op_to_yaml(op, {})
        assert out["action"] == "type_cast"
        assert out["source"] == "raw_price"

    def test_cast_resolves_source_through_column_mapping(self):
        op = {
            "primitive": "CAST",
            "source_column": "raw",
            "target_column": "price_usd",
            "target_type": "float",
        }
        # column_mapping renames raw → price_raw before yaml ops run
        out = _llm_op_to_yaml(op, {"raw": "price_raw"})
        assert out["source"] == "price_raw"

    def test_cast_without_source_returns_none(self):
        op = {"primitive": "CAST", "target_column": "x", "target_type": "float"}
        assert _llm_op_to_yaml(op, {}) is None

    def test_format_to_lowercase(self):
        op = {
            "primitive": "FORMAT",
            "action": "to_lowercase",
            "source_column": "name",
            "target_column": "name_lc",
        }
        out = _llm_op_to_yaml(op, {})
        assert out["action"] == "to_lowercase"

    def test_delete(self):
        op = {"primitive": "DELETE", "source_column": "junk"}
        out = _llm_op_to_yaml(op, {})
        assert out == {"source": "junk", "action": "drop_column"}

    def test_unify_coalesce(self):
        op = {
            "primitive": "UNIFY",
            "action": "coalesce",
            "sources": ["a", "b"],
            "target_column": "name",
            "target_type": "string",
        }
        out = _llm_op_to_yaml(op, {})
        assert out["action"] == "coalesce"
        assert out["sources"] == ["a", "b"]

    def test_split_json_array(self):
        op = {
            "primitive": "SPLIT",
            "action": "json_array_extract_multi",
            "source_column": "nutrients",
            "target_columns": {"protein_g": {"key": "amount", "filter": {"name": "Protein"}}},
        }
        out = _llm_op_to_yaml(op, {})
        assert out["action"] == "json_array_extract_multi"
        assert "protein_g" in out["target_columns"]

    def test_derive_expression(self):
        op = {
            "primitive": "DERIVE",
            "action": "expression",
            "expression": "price * qty",
            "sources": ["price", "qty"],
            "target_column": "total",
            "target_type": "float",
        }
        out = _llm_op_to_yaml(op, {})
        assert out["expression"] == "price * qty"

    def test_derive_unknown_action_returns_none(self):
        op = {
            "primitive": "DERIVE",
            "action": "telepathy",
            "source_column": "x",
            "target_column": "y",
        }
        assert _llm_op_to_yaml(op, {}) is None


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------


class TestConfidence:
    def test_low_null_high_confidence(self):
        score = calculate_confidence(
            null_rate=0.05,
            unique_count=100,
            sample_size=1000,
            has_source_column=True,
        )
        assert score.score > 0.7
        assert "low_null_rate" in score.factors

    def test_high_null_low_confidence(self):
        score = calculate_confidence(
            null_rate=0.95,
            unique_count=5,
            sample_size=50,
            has_source_column=False,
        )
        assert score.score < 0.3

    def test_no_source_column_penalty(self):
        with_source = calculate_confidence(0.0, 100, 1000, has_source_column=True)
        without = calculate_confidence(0.0, 100, 1000, has_source_column=False)
        assert with_source.score > without.score

    def test_confidence_score_in_unit_range(self):
        for nr in (0.0, 0.25, 0.5, 0.75, 1.0):
            score = calculate_confidence(nr, 50, 500)
            assert 0.0 <= score.score <= 1.0

    def test_confidence_level_high(self):
        assert get_confidence_level(0.95) == "high"

    def test_confidence_level_medium(self):
        assert get_confidence_level(0.7) == "medium"

    def test_confidence_level_low(self):
        assert get_confidence_level(0.3) == "low"

    def test_confidence_display_returns_tuple(self):
        icon, label = get_confidence_display(0.95)
        assert "High" in label

    def test_json_structure_lower_than_scalar(self):
        scalar = calculate_confidence(0.0, 100, 1000, detected_structure="scalar")
        json_arr = calculate_confidence(0.0, 100, 1000, detected_structure="json_array")
        assert scalar.score >= json_arr.score


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestOrchestratorConstants:
    def test_block_providers_includes_safety(self):
        # Safety columns must have provider entries so they are NOT marked unresolvable
        assert "allergens" in _BLOCK_COLUMN_PROVIDERS
        assert _BLOCK_COLUMN_PROVIDERS["allergens"] == "extract_allergens"
        assert _BLOCK_COLUMN_PROVIDERS["primary_category"] == "llm_enrich"

    def test_identity_columns_for_dedup(self):
        # These three are the dedup blocking key inputs
        assert "product_name" in _IDENTITY_COLUMNS
        assert "brand_owner" in _IDENTITY_COLUMNS
        assert "brand_name" in _IDENTITY_COLUMNS

    def test_dtype_family_covers_common_pandas_dtypes(self):
        assert _DTYPE_FAMILY["object"] == "string"
        assert _DTYPE_FAMILY["float64"] == "float"
        assert _DTYPE_FAMILY["int64"] == "integer"
