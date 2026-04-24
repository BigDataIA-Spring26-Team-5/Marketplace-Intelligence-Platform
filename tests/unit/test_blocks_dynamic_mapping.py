"""Unit tests for DynamicMappingBlock — YAML-driven declarative actions."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from src.blocks.dynamic_mapping import DynamicMappingBlock


def _write_yaml(path: Path, ops: list[dict], apply_if: str | None = None) -> str:
    data = {"column_operations": ops}
    if apply_if:
        data["apply_if_column_present"] = apply_if
    path.write_text(yaml.dump(data))
    return str(path)


class TestSetNull:
    def test_creates_null_column(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_null", "target": "x", "type": "string"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1, 2]})
        out = b.run(df)
        assert "x" in out.columns
        assert out["x"].isna().all()

    def test_skips_if_existing_data(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_null", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": ["keep"]})
        out = b.run(df)
        assert out["x"].iloc[0] == "keep"

    def test_force_overrides(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_null", "target": "x", "force": True}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": ["keep"]})
        out = b.run(df)
        assert out["x"].isna().all()


class TestSetDefault:
    def test_creates_with_default(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_default", "target": "flag", "type": "boolean", "default_value": True}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1, 2]})
        out = b.run(df)
        assert out["flag"].tolist() == [True, True]

    def test_falls_back_to_null_if_no_default(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_default", "target": "x", "type": "string"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"a": [1]}))
        assert out["x"].isna().all()


class TestRename:
    def test_renames_column(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "rename", "source": "a", "target": "b"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1, 2]})
        out = b.run(df)
        assert "b" in out.columns
        assert "a" not in out.columns

    def test_rename_missing_source_noop(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "rename", "source": "missing", "target": "b"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1]})
        out = b.run(df)
        assert "b" not in out.columns


class TestTypeCast:
    def test_cast_to_float(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "type_cast", "source": "x", "target": "x", "type": "float"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": ["1.5", "2.0", "bad"]})
        out = b.run(df)
        assert out["x"].iloc[0] == 1.5
        assert pd.isna(out["x"].iloc[2])

    def test_cast_to_integer(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "type_cast", "source": "x", "target": "x", "type": "integer"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": ["1", "2"]})
        out = b.run(df)
        assert str(out["x"].dtype) == "Int64"

    def test_cast_to_boolean(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "type_cast", "source": "x", "target": "x", "type": "boolean"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": ["true", "false", "yes", "0"]})
        out = b.run(df)
        assert out["x"].iloc[0] is True or out["x"].iloc[0] == True
        assert out["x"].iloc[3] is False or out["x"].iloc[3] == False

    def test_cast_missing_source_fallback(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "type_cast", "source": "missing", "target": "x", "type": "string"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"a": [1]}))
        assert "x" in out.columns
        assert out["x"].isna().all()


class TestDropColumn:
    def test_drops(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "drop_column", "source": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"x": [1], "y": [2]})
        out = b.run(df)
        assert "x" not in out.columns
        assert "y" in out.columns


class TestFormatOps:
    def test_to_lowercase(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "to_lowercase", "source": "x", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["ABC", "DEF"]}))
        assert out["x"].tolist() == ["abc", "def"]

    def test_to_uppercase(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "to_uppercase", "source": "x", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["abc"]}))
        assert out["x"].iloc[0] == "ABC"

    def test_strip_whitespace(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "strip_whitespace", "source": "x", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["  hi  "]}))
        assert out["x"].iloc[0] == "hi"

    def test_parse_date(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "parse_date", "source": "x", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["2024-01-01"]}))
        assert pd.notna(out["x"].iloc[0])

    def test_parse_unix_timestamp(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "parse_date", "source": "x", "target": "x", "format": "unix_timestamp"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": [1700000000]}))
        assert pd.notna(out["x"].iloc[0])

    def test_regex_replace(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "regex_replace", "source": "x", "target": "x", "pattern": r"\d+", "replacement": "N"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["abc123def"]}))
        assert out["x"].iloc[0] == "abcNdef"

    def test_regex_extract(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "regex_extract", "source": "x", "target": "y", "pattern": r"\d+"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["abc123def"]}))
        assert out["y"].iloc[0] == "123"

    def test_truncate_string(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "truncate_string", "source": "x", "target": "x", "max_length": 3}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["abcdef"]}))
        assert out["x"].iloc[0] == "abc"

    def test_pad_string_left_zero(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "pad_string", "source": "x", "target": "x", "min_length": 5}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["12"]}))
        assert out["x"].iloc[0] == "00012"

    def test_value_map(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "value_map", "source": "x", "target": "x",
            "mapping": {"a": "A", "b": "B"},
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["a", "b", "c"]}))
        assert out["x"].iloc[0] == "A"
        assert out["x"].iloc[2] == "c"  # passthrough


class TestUnifyOps:
    def test_coalesce(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "coalesce", "sources": ["a", "b"], "target": "c", "type": "string",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"a": [None, "x"], "b": ["y", "z"]}))
        assert out["c"].iloc[0] == "y"
        assert out["c"].iloc[1] == "x"

    def test_concat_columns(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "concat_columns", "sources": ["a", "b"], "target": "c", "separator": "-",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"a": ["x", "p"], "b": ["y", "q"]}))
        assert out["c"].iloc[0] == "x-y"

    def test_string_template(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "string_template", "target": "full", "template": "{first} {last}",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"first": ["John"], "last": ["Doe"]}))
        assert out["full"].iloc[0] == "John Doe"


class TestDeriveOps:
    def test_extract_json_field_object(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "extract_json_field", "source": "j", "target": "v", "key": "a",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"j": ['{"a": "hi"}']}))
        assert out["v"].iloc[0] == "hi"

    def test_conditional_map(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "conditional_map", "source": "x", "target": "y",
            "mapping": {"organic": "YES", "natural": "NO"}, "default": "UNK",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["100% organic apples", "unknown thing"]}))
        assert out["y"].iloc[0] == "YES"
        assert out["y"].iloc[1] == "UNK"

    def test_expression(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "expression", "expression": "a + b", "target": "c", "type": "float",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"a": [1, 2], "b": [10, 20]}))
        assert out["c"].iloc[0] == 11

    def test_contains_flag(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "contains_flag", "source": "txt", "target": "has_organic",
            "keywords": ["organic", "natural"],
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"txt": ["Fresh organic apples", "regular food"]}))
        assert out["has_organic"].iloc[0] == True
        assert out["has_organic"].iloc[1] == False


class TestSplitOps:
    def test_split_column(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "split_column", "source": "s", "target": "out",
            "delimiter": ",", "column_names": ["a", "b"],
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"s": ["x, y"]}))
        assert out["a"].iloc[0] == "x"
        assert out["b"].iloc[0] == "y"

    def test_xml_extract(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "xml_extract", "source": "x", "target": "v", "tag": "name",
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"x": ["<name>hi</name>"]}))
        assert out["v"].iloc[0] == "hi"

    def test_json_array_extract_multi(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{
            "action": "json_array_extract_multi", "source": "arr",
            "target_columns": {
                "protein": {"key": "amount", "filter": {"name": "protein"}, "type": "float"},
            },
        }])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        out = b.run(pd.DataFrame({"arr": ['[{"name":"protein","amount":10}]']}))
        assert out["protein"].iloc[0] == 10.0


class TestApplyIfGate:
    def test_skips_if_gate_column_missing(self, tmp_path):
        p = _write_yaml(
            tmp_path / "m.yaml",
            [{"action": "set_null", "target": "x"}],
            apply_if="required_col",
        )
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1]})
        out = b.run(df)
        assert "x" not in out.columns

    def test_runs_if_gate_present(self, tmp_path):
        p = _write_yaml(
            tmp_path / "m.yaml",
            [{"action": "set_null", "target": "x"}],
            apply_if="a",
        )
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        df = pd.DataFrame({"a": [1]})
        out = b.run(df)
        assert "x" in out.columns


class TestBlockInit:
    def test_inputs_and_outputs_populated(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [
            {"action": "rename", "source": "a", "target": "b"},
            {"action": "type_cast", "source": "c", "target": "d", "type": "float"},
        ])
        b = DynamicMappingBlock(domain="dom", yaml_path=p)
        assert "a" in b.inputs
        assert "b" in b.outputs
        assert b.domain == "dom"

    def test_name_from_file_stem(self, tmp_path):
        p = _write_yaml(tmp_path / "DYNAMIC_MAPPING_test.yaml", [{"action": "set_null", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        assert b.name == "DYNAMIC_MAPPING_test"

    def test_operations_property(self, tmp_path):
        p = _write_yaml(tmp_path / "m.yaml", [{"action": "set_null", "target": "x"}])
        b = DynamicMappingBlock(domain="d", yaml_path=p)
        assert len(b.operations) == 1
