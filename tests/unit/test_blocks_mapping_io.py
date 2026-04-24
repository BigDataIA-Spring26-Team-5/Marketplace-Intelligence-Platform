"""Unit tests for mapping_io — YAML read/write/validate."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.blocks import mapping_io
from src.blocks.mapping_io import (
    write_mapping_yaml,
    read_mapping_yaml,
    merge_hitl_decisions,
    VALID_ACTIONS,
)


class TestWriteMappingYaml:
    def test_writes_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mapping_io, "_GENERATED_DIR", tmp_path)
        ops = [{"action": "set_null", "target": "col1", "type": "string"}]
        p = write_mapping_yaml("nutrition", "test_src", ops)
        assert p.exists()
        assert "DYNAMIC_MAPPING_test_src.yaml" in p.name

    def test_sanitizes_slash_in_name(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mapping_io, "_GENERATED_DIR", tmp_path)
        p = write_mapping_yaml("d", "a/b", [{"action": "set_null", "target": "x"}])
        assert "/" not in p.name[len("DYNAMIC_MAPPING_"):]

    def test_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mapping_io, "_GENERATED_DIR", tmp_path)
        ops = [
            {"action": "rename", "source": "a", "target": "b"},
            {"action": "type_cast", "source": "x", "target": "y", "type": "float"},
        ]
        p = write_mapping_yaml("dom", "src", ops)
        loaded = read_mapping_yaml(p)
        assert len(loaded) == 2
        assert loaded[0]["action"] == "rename"


class TestReadMappingYaml:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_mapping_yaml(tmp_path / "nope.yaml")

    def test_invalid_root_missing_key(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"other": []}))
        with pytest.raises(ValueError, match="column_operations"):
            read_mapping_yaml(p)

    def test_operations_not_list(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": "not a list"}))
        with pytest.raises(ValueError, match="must be a list"):
            read_mapping_yaml(p)

    def test_missing_action_field(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [{"target": "x"}]}))
        with pytest.raises(ValueError, match="action"):
            read_mapping_yaml(p)

    def test_invalid_action(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [{"action": "bogus_action", "target": "x"}]}))
        with pytest.raises(ValueError, match="invalid action"):
            read_mapping_yaml(p)

    def test_missing_source_for_rename(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [{"action": "rename", "target": "x"}]}))
        with pytest.raises(ValueError, match="source"):
            read_mapping_yaml(p)

    def test_missing_sources_for_coalesce(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [{"action": "coalesce", "target": "x"}]}))
        with pytest.raises(ValueError, match="sources"):
            read_mapping_yaml(p)

    def test_missing_target_columns_for_json_extract(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [
            {"action": "json_array_extract_multi", "source": "x"}
        ]}))
        with pytest.raises(ValueError, match="target_columns"):
            read_mapping_yaml(p)

    def test_drop_column_no_target_ok(self, tmp_path):
        p = tmp_path / "ok.yaml"
        p.write_text(yaml.dump({"column_operations": [{"action": "drop_column", "source": "x"}]}))
        ops = read_mapping_yaml(p)
        assert ops[0]["action"] == "drop_column"

    def test_missing_target_for_set_null(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"column_operations": [{"action": "set_null"}]}))
        with pytest.raises(ValueError, match="target"):
            read_mapping_yaml(p)


class TestMergeHitlDecisions:
    def test_set_default(self):
        ops = [{"action": "set_null", "target": "col1", "type": "string"}]
        decisions = {"col1": {"action": "set_default", "value": "unknown"}}
        result = merge_hitl_decisions(ops, decisions)
        assert result[0]["action"] == "set_default"
        assert result[0]["default_value"] == "unknown"

    def test_accept_null_keeps_op(self):
        ops = [{"action": "set_null", "target": "col1"}]
        decisions = {"col1": {"action": "accept_null"}}
        result = merge_hitl_decisions(ops, decisions)
        assert result[0]["action"] == "set_null"

    def test_exclude_keeps_op(self):
        ops = [{"action": "set_null", "target": "col1"}]
        decisions = {"col1": {"action": "exclude"}}
        result = merge_hitl_decisions(ops, decisions)
        assert len(result) == 1

    def test_no_decision_passthrough(self):
        ops = [{"action": "rename", "source": "a", "target": "b"}]
        result = merge_hitl_decisions(ops, {})
        assert result == ops


class TestValidActions:
    def test_valid_actions_nonempty(self):
        assert "set_null" in VALID_ACTIONS
        assert "rename" in VALID_ACTIONS
        assert "coalesce" in VALID_ACTIONS
