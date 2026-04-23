"""Tests for src/blocks/dynamic_mapping.py — declarative YAML actions."""

from __future__ import annotations

import pandas as pd
import pytest

from src.blocks.dynamic_mapping import (
    _ACTION_HANDLERS,
    _cast_value,
    _try_parse,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestTryParse:
    def test_parses_json_object(self):
        assert _try_parse('{"a": 1}') == {"a": 1}

    def test_parses_json_array(self):
        assert _try_parse('[1, 2, 3]') == [1, 2, 3]

    def test_falls_back_to_python_repr(self):
        assert _try_parse("[1, 2, 3]") == [1, 2, 3]

    def test_returns_none_for_plain_string(self):
        assert _try_parse("hello") is None

    def test_returns_none_for_invalid(self):
        assert _try_parse("not parseable {") is None


class TestCastValue:
    def test_float(self):
        assert _cast_value("3.14", "float") == 3.14

    def test_integer(self):
        assert _cast_value("42", "integer") == 42

    def test_boolean_truthy(self):
        assert _cast_value("yes", "boolean") is True
        assert _cast_value("true", "boolean") is True
        assert _cast_value("1", "boolean") is True

    def test_boolean_falsy(self):
        assert _cast_value("no", "boolean") is False
        assert _cast_value("false", "boolean") is False

    def test_string_default(self):
        assert _cast_value(123, "string") == "123"


# ---------------------------------------------------------------------------
# Scalar creation
# ---------------------------------------------------------------------------


class TestSetNullSetDefault:
    def test_set_null_creates_typed_na_column(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        out = _ACTION_HANDLERS["set_null"](df, {"target": "y", "type": "float"})
        assert "y" in out.columns
        assert out["y"].isna().all()

    def test_set_default_creates_constant_column(self):
        df = pd.DataFrame({"x": [1, 2]})
        out = _ACTION_HANDLERS["set_default"](
            df, {"target": "y", "type": "string", "default_value": "USA"}
        )
        assert (out["y"] == "USA").all()

    def test_set_default_with_none_falls_back_to_set_null(self):
        df = pd.DataFrame({"x": [1, 2]})
        out = _ACTION_HANDLERS["set_default"](
            df, {"target": "y", "type": "float", "default_value": None}
        )
        assert out["y"].isna().all()


# ---------------------------------------------------------------------------
# Type ops
# ---------------------------------------------------------------------------


class TestTypeCast:
    def test_string_to_float(self):
        df = pd.DataFrame({"x": ["1.5", "2.5", "bad"]})
        out = _ACTION_HANDLERS["type_cast"](
            df, {"source": "x", "target": "y", "type": "float"}
        )
        assert out["y"].iloc[0] == 1.5
        assert pd.isna(out["y"].iloc[2])

    def test_string_to_int(self):
        df = pd.DataFrame({"x": ["10", "20"]})
        out = _ACTION_HANDLERS["type_cast"](
            df, {"source": "x", "target": "y", "type": "integer"}
        )
        assert out["y"].iloc[0] == 10
        assert str(out["y"].dtype) == "Int64"

    def test_string_to_boolean(self):
        df = pd.DataFrame({"x": ["yes", "no", "true", "false", "weird"]})
        out = _ACTION_HANDLERS["type_cast"](
            df, {"source": "x", "target": "y", "type": "boolean"}
        )
        assert out["y"].iloc[0] is True or bool(out["y"].iloc[0]) is True
        assert pd.isna(out["y"].iloc[4])

    def test_missing_source_falls_back_to_null(self):
        df = pd.DataFrame({"a": [1]})
        out = _ACTION_HANDLERS["type_cast"](
            df, {"source": "missing", "target": "y", "type": "string"}
        )
        assert "y" in out.columns
        assert out["y"].isna().all()


class TestRenameDrop:
    def test_rename(self):
        df = pd.DataFrame({"old": [1, 2]})
        out = _ACTION_HANDLERS["rename"](df, {"source": "old", "target": "new"})
        assert "new" in out.columns
        assert "old" not in out.columns

    def test_rename_skips_when_source_missing(self):
        df = pd.DataFrame({"x": [1]})
        out = _ACTION_HANDLERS["rename"](df, {"source": "missing", "target": "new"})
        assert "new" not in out.columns

    def test_drop_column(self):
        df = pd.DataFrame({"x": [1], "y": [2]})
        out = _ACTION_HANDLERS["drop_column"](df, {"source": "x"})
        assert "x" not in out.columns
        assert "y" in out.columns


# ---------------------------------------------------------------------------
# Format ops
# ---------------------------------------------------------------------------


class TestFormatOps:
    def test_to_lowercase(self):
        df = pd.DataFrame({"x": ["HELLO", "World"]})
        out = _ACTION_HANDLERS["to_lowercase"](
            df, {"source": "x", "target": "y"}
        )
        assert out["y"].iloc[0] == "hello"

    def test_to_uppercase(self):
        df = pd.DataFrame({"x": ["hello"]})
        out = _ACTION_HANDLERS["to_uppercase"](
            df, {"source": "x", "target": "y"}
        )
        assert out["y"].iloc[0] == "HELLO"

    def test_strip_whitespace(self):
        df = pd.DataFrame({"x": ["  hi  ", " world "]})
        out = _ACTION_HANDLERS["strip_whitespace"](
            df, {"source": "x", "target": "y"}
        )
        assert out["y"].iloc[0] == "hi"

    def test_regex_replace(self):
        df = pd.DataFrame({"x": ["abc123", "xyz456"]})
        out = _ACTION_HANDLERS["regex_replace"](
            df, {"source": "x", "target": "y", "pattern": r"\d+", "replacement": "#"}
        )
        assert out["y"].iloc[0] == "abc#"

    def test_regex_extract_first_number(self):
        df = pd.DataFrame({"x": ["abc123def", "no-numbers"]})
        out = _ACTION_HANDLERS["regex_extract"](
            df, {"source": "x", "target": "y", "pattern": r"\d+", "type": "string"}
        )
        assert out["y"].iloc[0] == "123"
        assert pd.isna(out["y"].iloc[1])

    def test_truncate_string(self):
        df = pd.DataFrame({"x": ["abcdefghij"]})
        out = _ACTION_HANDLERS["truncate_string"](
            df, {"source": "x", "target": "y", "max_length": 4}
        )
        assert out["y"].iloc[0] == "abcd"

    def test_pad_string_left_with_zero(self):
        df = pd.DataFrame({"x": ["42", "9"]})
        out = _ACTION_HANDLERS["pad_string"](
            df, {"source": "x", "target": "y", "min_length": 4, "fill_char": "0"}
        )
        assert out["y"].iloc[0] == "0042"
        assert out["y"].iloc[1] == "0009"

    def test_value_map_with_default(self):
        df = pd.DataFrame({"x": ["yes", "no", "weird"]})
        out = _ACTION_HANDLERS["value_map"](
            df,
            {
                "source": "x",
                "target": "y",
                "type": "boolean",
                "mapping": {"yes": True, "no": False},
                "default": False,
            },
        )
        assert out["y"].iloc[0] is True or bool(out["y"].iloc[0]) is True
        assert bool(out["y"].iloc[2]) is False


# ---------------------------------------------------------------------------
# Unify ops
# ---------------------------------------------------------------------------


class TestUnifyOps:
    def test_coalesce_picks_first_non_null(self):
        df = pd.DataFrame(
            {
                "a": [None, "x", None],
                "b": ["y", None, None],
                "c": ["z", "z", "z"],
            }
        )
        out = _ACTION_HANDLERS["coalesce"](
            df, {"sources": ["a", "b", "c"], "target": "result", "type": "string"}
        )
        assert out["result"].iloc[0] == "y"
        assert out["result"].iloc[1] == "x"
        assert out["result"].iloc[2] == "z"

    def test_concat_columns_with_separator(self):
        df = pd.DataFrame({"a": ["foo", "x"], "b": ["bar", None]})
        out = _ACTION_HANDLERS["concat_columns"](
            df,
            {
                "sources": ["a", "b"],
                "target": "c",
                "separator": " - ",
                "exclude_nulls": True,
            },
        )
        assert out["c"].iloc[0] == "foo - bar"
        assert out["c"].iloc[1] == "x"  # null b excluded

    def test_string_template(self):
        df = pd.DataFrame({"first_name": ["alice"], "last_name": ["smith"]})
        out = _ACTION_HANDLERS["string_template"](
            df, {"template": "{first_name} {last_name}", "target": "full_name"}
        )
        assert out["full_name"].iloc[0] == "alice smith"


# ---------------------------------------------------------------------------
# Derive ops
# ---------------------------------------------------------------------------


class TestDeriveOps:
    def test_extract_json_field_from_object(self):
        df = pd.DataFrame({"meta": ['{"size": "10g", "color": "red"}']})
        out = _ACTION_HANDLERS["extract_json_field"](
            df, {"source": "meta", "target": "size", "key": "size", "type": "string"}
        )
        assert out["size"].iloc[0] == "10g"

    def test_extract_json_field_from_array_with_filter(self):
        df = pd.DataFrame(
            {"meta": ['[{"type": "x", "v": 1}, {"type": "y", "v": 2}]']}
        )
        out = _ACTION_HANDLERS["extract_json_field"](
            df,
            {
                "source": "meta",
                "target": "v",
                "key": "v",
                "filter": {"type": "y"},
                "type": "integer",
            },
        )
        assert out["v"].iloc[0] == 2

    def test_conditional_map_keyword_match(self):
        df = pd.DataFrame({"name": ["chocolate bar", "salted nuts", "plain water"]})
        out = _ACTION_HANDLERS["conditional_map"](
            df,
            {
                "source": "name",
                "target": "category",
                "mapping": {"chocolate": "Confectionery", "nuts": "Snacks"},
                "default": "Other",
                "type": "string",
            },
        )
        assert out["category"].iloc[0] == "Confectionery"
        assert out["category"].iloc[1] == "Snacks"
        assert out["category"].iloc[2] == "Other"

    def test_expression_arithmetic(self):
        df = pd.DataFrame({"a": [10, 20], "b": [2, 4]})
        out = _ACTION_HANDLERS["expression"](
            df, {"expression": "a * b", "target": "c", "type": "float"}
        )
        assert out["c"].iloc[0] == 20.0
        assert out["c"].iloc[1] == 80.0

    def test_expression_invalid_falls_back_to_null(self):
        df = pd.DataFrame({"a": [1]})
        out = _ACTION_HANDLERS["expression"](
            df, {"expression": "nonsense @ ?", "target": "c", "type": "float"}
        )
        assert out["c"].isna().all()

    def test_contains_flag(self):
        df = pd.DataFrame({"x": ["may contain milk", "wheat free", None]})
        out = _ACTION_HANDLERS["contains_flag"](
            df, {"source": "x", "target": "has_milk", "keywords": ["milk"]}
        )
        assert bool(out["has_milk"].iloc[0]) is True
        assert bool(out["has_milk"].iloc[1]) is False
        assert pd.isna(out["has_milk"].iloc[2]) or bool(out["has_milk"].iloc[2]) is False
