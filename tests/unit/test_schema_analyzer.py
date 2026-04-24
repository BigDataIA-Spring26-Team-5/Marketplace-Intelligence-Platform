"""Unit tests for schema analyzer — structure detection and diffing."""

from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
import pytest

from src.schema import analyzer
from src.schema.analyzer import (
    _try_parse_json,
    _detect_structure,
    _parse_json_samples,
    _infer_keys_and_types,
    _count_components,
    _candidate_unify_groups,
    profile_dataframe,
    compute_schema_diff,
    derive_unified_schema_from_source,
    get_domain_schema,
    save_domain_schema,
    _reset_schema_cache,
)
from src.schema.models import UnifiedSchema, ColumnSpec


class TestTryParseJson:
    def test_valid_json_object(self):
        assert _try_parse_json('{"a": 1}') == {"a": 1}

    def test_valid_json_array(self):
        assert _try_parse_json('[1, 2]') == [1, 2]

    def test_python_repr_dict(self):
        assert _try_parse_json("{'a': 1}") == {"a": 1}

    def test_invalid_returns_none(self):
        assert _try_parse_json("not json") is None

    def test_scalar_rejected(self):
        assert _try_parse_json("42") is None  # int, not dict/list


class TestDetectStructure:
    def test_empty_series(self):
        s = pd.Series([None, None], dtype=object)
        assert _detect_structure(s) == "scalar"

    def test_json_array(self):
        s = pd.Series(['[1,2]', '[3,4]', '[5]'] * 10)
        assert _detect_structure(s) == "json_array"

    def test_json_object(self):
        s = pd.Series(['{"a": 1}', '{"b": 2}'] * 10)
        assert _detect_structure(s) == "json_object"

    def test_delimited_pipe(self):
        s = pd.Series(["a|b|c", "x|y|z"] * 10)
        assert _detect_structure(s) == "delimited"

    def test_composite(self):
        s = pd.Series(["100 grams", "200 ml", "50 oz"] * 10)
        assert _detect_structure(s) == "composite"

    def test_xml(self):
        s = pd.Series(["<foo>x</foo>", "<bar>y</bar>"] * 10)
        assert _detect_structure(s) == "xml"

    def test_scalar_fallback(self):
        s = pd.Series(["hello", "world", "foo"])
        assert _detect_structure(s) == "scalar"


class TestParseJsonSamples:
    def test_returns_up_to_n(self):
        s = pd.Series(['{"a": 1}', '{"b": 2}', '{"c": 3}', '{"d": 4}'])
        result = _parse_json_samples(s, n=2)
        assert len(result) == 2


class TestInferKeysAndTypes:
    def test_non_json_returns_empty(self):
        s = pd.Series(["a", "b"])
        keys, types = _infer_keys_and_types(s, "scalar")
        assert keys == [] and types == {}

    def test_json_object(self):
        s = pd.Series(['{"a": 1, "b": "s"}', '{"a": 2, "b": "t"}'])
        keys, types = _infer_keys_and_types(s, "json_object")
        assert "a" in keys and "b" in keys
        assert types["a"] == "integer"
        assert types["b"] == "string"

    def test_json_array_of_dicts(self):
        s = pd.Series(['[{"k": true}]', '[{"k": false}]'])
        keys, types = _infer_keys_and_types(s, "json_array")
        assert types["k"] == "boolean"

    def test_mixed_types(self):
        s = pd.Series(['{"a": 1}', '{"a": "str"}'])
        _, types = _infer_keys_and_types(s, "json_object")
        assert types["a"] == "mixed"

    def test_all_none_values(self):
        s = pd.Series(['{"a": null}', '{"a": null}'])
        _, types = _infer_keys_and_types(s, "json_object")
        assert types["a"] == "null"

    def test_float_type(self):
        s = pd.Series(['{"a": 1.5}', '{"a": 2.0}'])
        _, types = _infer_keys_and_types(s, "json_object")
        assert types["a"] == "float"


class TestCountComponents:
    def test_json_array(self):
        s = pd.Series(['[1,2,3]', '[4,5,6]'])
        assert _count_components(s, "json_array") == 3

    def test_json_object(self):
        s = pd.Series(['{"a":1,"b":2}', '{"c":3,"d":4}'])
        assert _count_components(s, "json_object") == 2

    def test_delimited(self):
        s = pd.Series(["a|b|c", "d|e|f"])
        assert _count_components(s, "delimited") == 3

    def test_composite(self):
        s = pd.Series(["1 g"])
        assert _count_components(s, "composite") == 2

    def test_scalar(self):
        s = pd.Series(["x"])
        assert _count_components(s, "scalar") == 1


class TestCandidateUnifyGroups:
    def test_numeric_suffix_group(self):
        profile = {"addr_1": {}, "addr_2": {}, "addr_3": {}, "name": {}}
        groups = _candidate_unify_groups(profile)
        addr_group = [g for g in groups if "addr_1" in g]
        assert len(addr_group) == 1
        assert set(addr_group[0]) == {"addr_1", "addr_2", "addr_3"}

    def test_amount_unit_pair(self):
        profile = {"serving_size": {}, "serving_size_unit": {}}
        groups = _candidate_unify_groups(profile)
        assert any(set(g) == {"serving_size", "serving_size_unit"} for g in groups)

    def test_no_groups(self):
        profile = {"a": {}, "b": {}}
        assert _candidate_unify_groups(profile) == []


class TestProfileDataframe:
    def test_basic_profile(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        p = profile_dataframe(df)
        assert "a" in p and "b" in p
        assert p["a"]["is_numeric"] is True
        assert p["b"]["is_numeric"] is False
        assert p["__meta__"]["row_count"] == 3
        assert "a" in p["__meta__"]["numeric_columns"]

    def test_null_rate_computed(self):
        df = pd.DataFrame({"a": [1, None, None, None]})
        p = profile_dataframe(df)
        assert p["a"]["null_rate"] == 0.75

    def test_json_column_has_parsed_sample(self):
        df = pd.DataFrame({"j": ['{"a": 1}'] * 10})
        p = profile_dataframe(df)
        assert p["j"]["detected_structure"] == "json_object"
        assert "parsed_sample" in p["j"]


class TestComputeSchemaDiff:
    def test_direct_match(self):
        schema = UnifiedSchema(columns={"product_name": ColumnSpec(type="string", required=True)})
        profile = {"product_name": {"dtype": "object"}, "__meta__": {}}
        mapping, gaps = compute_schema_diff(profile, schema)
        assert mapping == {"product_name": "product_name"}
        assert gaps == []

    def test_missing_target_gap(self):
        schema = UnifiedSchema(columns={"product_name": ColumnSpec(type="string", required=True)})
        profile = {"other_col": {"dtype": "object"}, "__meta__": {}}
        mapping, gaps = compute_schema_diff(profile, schema)
        assert mapping == {}
        assert len(gaps) == 1
        assert gaps[0]["target_column"] == "product_name"
        assert gaps[0]["action"] == "ADD"


class TestDomainSchemaCache:
    def test_missing_domain_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(analyzer, "SCHEMAS_DIR", tmp_path)
        _reset_schema_cache()
        with pytest.raises(FileNotFoundError):
            get_domain_schema("nonexistent_domain_xyz")

    def test_save_and_load(self, tmp_path, monkeypatch):
        monkeypatch.setattr(analyzer, "SCHEMAS_DIR", tmp_path)
        _reset_schema_cache()
        schema = UnifiedSchema(columns={"x": ColumnSpec(type="string", required=True)})
        save_domain_schema(schema, "testdom")
        loaded = get_domain_schema("testdom")
        assert "x" in loaded.columns

    def test_reset_clears_all(self):
        _reset_schema_cache()
        _reset_schema_cache("anything")  # should not error

    def test_reset_specific(self, tmp_path, monkeypatch):
        monkeypatch.setattr(analyzer, "SCHEMAS_DIR", tmp_path)
        _reset_schema_cache()
        schema = UnifiedSchema(columns={"x": ColumnSpec(type="string")})
        save_domain_schema(schema, "td2")
        get_domain_schema("td2")
        _reset_schema_cache("td2")
        assert "td2" not in analyzer._schema_cache


class TestDeriveUnifiedSchema:
    def test_derives_from_df(self, tmp_path, monkeypatch):
        monkeypatch.setattr(analyzer, "SCHEMAS_DIR", tmp_path)
        _reset_schema_cache()
        df = pd.DataFrame({
            "name": ["a", "b"],
            "price": [1.5, 2.0],
            "count": [1, 2],
            "flag": [True, False],
        })
        mapping = {"name": "product_name", "price": "price", "count": "count", "flag": "flag"}
        schema = derive_unified_schema_from_source(df, mapping, "d1")
        assert "product_name" in schema.columns
        assert schema.columns["price"].type == "float"
        assert schema.columns["count"].type == "integer"
        assert schema.columns["flag"].type == "boolean"
        # Enrichment cols added
        assert "allergens" in schema.columns
        assert "primary_category" in schema.columns
        assert schema.columns["is_organic"].type == "boolean"
        # Computed cols added
        assert "dq_score_pre" in schema.columns
        assert schema.columns["dq_score_pre"].computed is True
