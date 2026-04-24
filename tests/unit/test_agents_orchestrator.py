"""Unit tests for src.agents.orchestrator (helpers + LLM-facing nodes)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.agents.orchestrator import (
    _to_snake,
    _detect_enrichment_columns,
    _parse_llm_response,
    _compute_schema_fingerprint,
    _llm_op_to_yaml,
    _deterministic_corrections,
    analyze_schema_node,
    load_source_node,
)


# ---------------------------------------------------------------------------
# _to_snake
# ---------------------------------------------------------------------------

class TestToSnake:
    def test_simple_lowercase(self):
        assert _to_snake("foo") == "foo"

    def test_camel_case(self):
        assert _to_snake("fooBar") == "foo_bar"

    def test_pascal_case(self):
        assert _to_snake("FooBar") == "foo_bar"

    def test_all_caps_then_word(self):
        assert _to_snake("HTTPServer") == "http_server"

    def test_with_space_and_hyphen(self):
        assert _to_snake("Foo Bar-Baz") == "foo_bar_baz"

    def test_already_snake(self):
        assert _to_snake("foo_bar") == "foo_bar"


# ---------------------------------------------------------------------------
# _detect_enrichment_columns
# ---------------------------------------------------------------------------

class TestDetectEnrichmentColumns:
    def test_enrichment_absent_from_source(self):
        unified = MagicMock()
        unified.enrichment_columns = ["primary_category", "allergens"]
        source = {"product_name": {}, "brand_name": {}}
        assert set(_detect_enrichment_columns(unified, source)) == {"primary_category", "allergens"}

    def test_enrichment_present_in_source(self):
        unified = MagicMock()
        unified.enrichment_columns = ["primary_category", "allergens"]
        source = {"product_name": {}, "allergens": {}}
        assert _detect_enrichment_columns(unified, source) == ["primary_category"]

    def test_excludes_meta_from_source(self):
        # __meta__ in source is stripped before comparison
        unified = MagicMock()
        unified.enrichment_columns = ["primary_category"]
        source = {"__meta__": {"sample_size": 100}, "product_name": {}}
        result = _detect_enrichment_columns(unified, source)
        assert result == ["primary_category"]


# ---------------------------------------------------------------------------
# _parse_llm_response
# ---------------------------------------------------------------------------

class TestParseLLMResponse:
    def test_new_format(self):
        result = {
            "column_mapping": {"a": "b"},
            "operations": [{"primitive": "RENAME"}],
            "unresolvable": [{"target_column": "x"}],
        }
        mapping, ops, un, legacy = _parse_llm_response(result)
        assert mapping == {"a": "b"}
        assert len(ops) == 1
        assert len(un) == 1
        assert legacy == []

    def test_legacy_format_derivable(self):
        result = {
            "column_mapping": {"a": "b"},
            "derivable_gaps": [{"target_column": "c", "source_column": "c_raw"}],
            "missing_columns": [{"target_column": "d"}],
        }
        mapping, ops, un, legacy = _parse_llm_response(result)
        assert mapping == {"a": "b"}
        assert ops == []
        # legacy combines derivable + missing
        assert len(legacy) == 2

    def test_flat_gaps_format(self):
        result = {"column_mapping": {}, "gaps": [{"target_column": "x"}]}
        mapping, ops, un, legacy = _parse_llm_response(result)
        assert legacy == [{"target_column": "x"}]

    def test_empty_result(self):
        mapping, ops, un, legacy = _parse_llm_response({})
        assert mapping == {}
        assert ops == []
        assert un == []
        assert legacy == []


# ---------------------------------------------------------------------------
# _compute_schema_fingerprint
# ---------------------------------------------------------------------------

class TestComputeFingerprint:
    def test_deterministic(self):
        s = {"a": {}, "b": {}}
        assert _compute_schema_fingerprint(s, "nutrition", "1") == _compute_schema_fingerprint(s, "nutrition", "1")

    def test_order_independent(self):
        s1 = {"a": {}, "b": {}}
        s2 = {"b": {}, "a": {}}
        assert _compute_schema_fingerprint(s1, "nutrition", "1") == _compute_schema_fingerprint(s2, "nutrition", "1")

    def test_different_domain_different_hash(self):
        s = {"a": {}}
        assert _compute_schema_fingerprint(s, "nutrition", "1") != _compute_schema_fingerprint(s, "safety", "1")

    def test_different_version_different_hash(self):
        s = {"a": {}}
        assert _compute_schema_fingerprint(s, "nutrition", "1") != _compute_schema_fingerprint(s, "nutrition", "2")

    def test_excludes_meta(self):
        s1 = {"a": {}}
        s2 = {"a": {}, "__meta__": {"x": 1}}
        assert _compute_schema_fingerprint(s1, "nutrition", "1") == _compute_schema_fingerprint(s2, "nutrition", "1")

    def test_length_is_16(self):
        assert len(_compute_schema_fingerprint({"a": {}}, "n", "1")) == 16


# ---------------------------------------------------------------------------
# _llm_op_to_yaml
# ---------------------------------------------------------------------------

class TestLLMOpToYaml:
    def test_add_set_null(self):
        op = {"primitive": "ADD", "action": "set_null", "target_column": "x", "target_type": "string"}
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "set_null"
        assert y["target"] == "x"

    def test_add_set_default(self):
        op = {
            "primitive": "ADD", "action": "set_default", "target_column": "x",
            "target_type": "int", "default_value": 0,
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "set_default"
        assert y["default_value"] == 0

    def test_cast(self):
        op = {
            "primitive": "CAST", "action": "type_cast", "source_column": "a",
            "target_column": "b", "target_type": "float",
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "type_cast"
        assert y["source"] == "a"

    def test_cast_no_source_returns_none(self):
        op = {"primitive": "CAST", "action": "type_cast", "target_column": "b"}
        assert _llm_op_to_yaml(op, {}) is None

    def test_format(self):
        op = {
            "primitive": "FORMAT", "action": "regex_replace",
            "source_column": "a", "target_column": "b",
            "pattern": r"\s", "replacement": "",
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "regex_replace"
        assert y["pattern"] == r"\s"
        assert y["replacement"] == ""

    def test_format_unknown_action_falls_back(self):
        op = {
            "primitive": "FORMAT", "action": "bogus",
            "source_column": "a", "target_column": "b",
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "format_transform"

    def test_format_no_source_returns_none(self):
        op = {"primitive": "FORMAT", "target_column": "b"}
        assert _llm_op_to_yaml(op, {}) is None

    def test_rename(self):
        op = {"primitive": "RENAME", "source_column": "a", "target_column": "b"}
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "rename"
        assert y["source"] == "a"

    def test_rename_no_source_returns_none(self):
        op = {"primitive": "RENAME", "target_column": "b"}
        assert _llm_op_to_yaml(op, {}) is None

    def test_delete(self):
        op = {"primitive": "DELETE", "source_column": "a"}
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "drop_column"
        assert y["source"] == "a"

    def test_delete_no_source_returns_none(self):
        assert _llm_op_to_yaml({"primitive": "DELETE"}, {}) is None

    def test_split_json_array(self):
        op = {
            "primitive": "SPLIT", "action": "json_array_extract_multi",
            "source_column": "arr", "target_columns": {"a": 0, "b": 1},
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "json_array_extract_multi"
        assert y["target_columns"] == {"a": 0, "b": 1}

    def test_split_column(self):
        op = {
            "primitive": "SPLIT", "action": "split_column",
            "source_column": "name", "column_names": ["first", "last"],
            "delimiter": " ",
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "split_column"
        assert y["column_names"] == ["first", "last"]

    def test_split_unknown_action_returns_none(self):
        op = {"primitive": "SPLIT", "action": "bogus", "source_column": "x"}
        assert _llm_op_to_yaml(op, {}) is None

    def test_unify_coalesce(self):
        op = {"primitive": "UNIFY", "action": "coalesce", "target_column": "x", "sources": ["a", "b"]}
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "coalesce"
        assert y["sources"] == ["a", "b"]

    def test_unify_concat(self):
        op = {
            "primitive": "UNIFY", "action": "concat_columns",
            "target_column": "full_name", "sources": ["first", "last"],
            "separator": "-",
        }
        y = _llm_op_to_yaml(op, {})
        assert y["action"] == "concat_columns"
        assert y["separator"] == "-"

    def test_unknown_primitive_returns_none(self):
        assert _llm_op_to_yaml({"primitive": "WAT"}, {}) is None

    def test_source_resolved_through_column_mapping(self):
        op = {"primitive": "CAST", "action": "type_cast", "source_column": "raw", "target_column": "t"}
        y = _llm_op_to_yaml(op, {"raw": "unified"})
        assert y["source"] == "unified"


# ---------------------------------------------------------------------------
# _deterministic_corrections
# ---------------------------------------------------------------------------

class TestDeterministicCorrections:
    def _make_schema(self, columns):
        unified = MagicMock()
        unified.columns = {}
        for name, typ in columns.items():
            spec = MagicMock()
            spec.type = typ
            unified.columns[name] = spec
        return unified

    def test_rule4_rename_type_mismatch_becomes_cast(self):
        source = {"price_raw": {"dtype": "object"}}
        ops = [{"primitive": "RENAME", "source_column": "price_raw", "target_column": "price"}]
        unified = self._make_schema({"price": "float"})
        out = _deterministic_corrections(ops, {"price_raw": "price"}, source, unified)
        # Rule 4 should reclassify RENAME→CAST when types mismatch
        cast_ops = [o for o in out if o.get("primitive") == "CAST"]
        assert len(cast_ops) == 1

    def test_rule4_rename_compatible_stays_rename(self):
        source = {"name_raw": {"dtype": "object"}}
        ops = [{"primitive": "RENAME", "source_column": "name_raw", "target_column": "name"}]
        unified = self._make_schema({"name": "string"})
        out = _deterministic_corrections(ops, {"name_raw": "name"}, source, unified)
        assert out[0]["primitive"] == "RENAME"

    def test_rule6_adds_delete_for_uncovered(self):
        source = {"a": {"dtype": "object"}, "b": {"dtype": "object"}, "unused": {"dtype": "object"}}
        ops = [{"primitive": "RENAME", "source_column": "a", "target_column": "a_t"}]
        unified = self._make_schema({"a_t": "string"})
        out = _deterministic_corrections(ops, {"b": "b_t"}, source, unified)
        delete_srcs = {o.get("source_column") for o in out if o.get("primitive") == "DELETE"}
        assert "unused" in delete_srcs

    def test_rule7_adds_normalize_before_dedup(self):
        source = {"name_raw": {"dtype": "object"}}
        ops = [{"primitive": "RENAME", "source_column": "name_raw", "target_column": "product_name"}]
        unified = self._make_schema({"product_name": "string"})
        out = _deterministic_corrections(ops, {"name_raw": "product_name"}, source, unified)
        assert out[0].get("normalize_before_dedup") is True

    def test_protects_provider_columns_from_delete(self):
        # allergens is a block-provided column; must not be DELETE'd even if in source
        source = {"allergens": {"dtype": "object"}}
        ops = [{"primitive": "DELETE", "source_column": "allergens"}]
        unified = self._make_schema({})
        out = _deterministic_corrections(ops, {}, source, unified)
        delete_srcs = {o.get("source_column") for o in out if o.get("primitive") == "DELETE"}
        assert "allergens" not in delete_srcs


# ---------------------------------------------------------------------------
# analyze_schema_node
# ---------------------------------------------------------------------------

class TestAnalyzeSchemaNode:
    def test_short_circuits_when_operations_present(self):
        state = {"operations": [{"primitive": "RENAME"}], "source_schema": {}}
        assert analyze_schema_node(state) == {}

    def test_cache_hit_returns_cached_state(self, tmp_path):
        cache_client = MagicMock()
        cached = {
            "column_mapping": {"a": "b"},
            "operations": [{"primitive": "RENAME"}],
            "mapping_yaml_path": str(tmp_path / "m.yaml"),
            "__yaml_text__": "version: 1\nops: []",
        }
        cache_client.get.return_value = json.dumps(cached).encode()

        state = {
            "source_schema": {"a": {"dtype": "object"}, "__meta__": {}},
            "domain": "nutrition",
            "cache_client": cache_client,
        }
        with patch("src.agents.orchestrator.get_domain_schema") as mock_schema:
            unified = MagicMock()
            unified.version = "1"
            unified.for_prompt.return_value = {}
            mock_schema.return_value = unified
            out = analyze_schema_node(state)

        assert out["cache_yaml_hit"] is True
        assert out["column_mapping"] == {"a": "b"}
        # YAML file was materialized to disk
        assert (tmp_path / "m.yaml").exists()

    def test_cache_miss_invokes_llm(self):
        cache_client = MagicMock()
        cache_client.get.return_value = None

        state = {
            "source_schema": {
                "raw_name": {"dtype": "object", "null_rate": 0.0, "unique_count": 10, "detected_structure": "scalar"},
                "__meta__": {"sampling_strategy": {"sample_size": 100}},
            },
            "domain": "nutrition",
            "cache_client": cache_client,
        }

        fake_llm_result = {
            "column_mapping": {"raw_name": "product_name"},
            "operations": [
                {"primitive": "RENAME", "source_column": "raw_name", "target_column": "product_name"},
            ],
        }

        with (
            patch("src.agents.orchestrator.call_llm_json", return_value=fake_llm_result),
            patch("src.agents.orchestrator.get_orchestrator_llm", return_value="mock-model"),
            patch("src.agents.orchestrator.get_domain_schema") as mock_schema,
        ):
            unified = MagicMock()
            unified.version = "1"
            unified.for_prompt.return_value = {}
            unified.required_columns = []
            unified.enrichment_columns = []
            unified.columns = {}
            mock_schema.return_value = unified
            out = analyze_schema_node(state)

        assert out["column_mapping"] == {"raw_name": "product_name"}
        assert len(out["operations"]) == 1
        assert out["_schema_fingerprint"]

    def test_cache_hit_deserialization_failure_runs_llm(self):
        cache_client = MagicMock()
        cache_client.get.return_value = b"not-valid-json"

        state = {
            "source_schema": {"a": {"dtype": "object"}, "__meta__": {}},
            "domain": "nutrition",
            "cache_client": cache_client,
        }
        with (
            patch("src.agents.orchestrator.call_llm_json", return_value={"column_mapping": {}, "operations": []}),
            patch("src.agents.orchestrator.get_orchestrator_llm", return_value="m"),
            patch("src.agents.orchestrator.get_domain_schema") as mock_schema,
        ):
            unified = MagicMock()
            unified.version = "1"
            unified.for_prompt.return_value = {}
            unified.required_columns = []
            unified.enrichment_columns = []
            unified.columns = {}
            mock_schema.return_value = unified
            out = analyze_schema_node(state)
        # Should still produce a result, not a cache hit
        assert "cache_yaml_hit" not in out


# ---------------------------------------------------------------------------
# load_source_node
# ---------------------------------------------------------------------------

class TestLoadSourceNode:
    def test_short_circuits_when_source_df_present(self):
        df = pd.DataFrame({"a": [1]})
        out = load_source_node({"source_df": df})
        assert out == {}

    def test_loads_local_csv(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text("name,value\nA,1\nB,2\nC,3\n")
        state = {"source_path": str(csv)}
        out = load_source_node(state)
        assert "source_df" in out
        assert "source_schema" in out
        assert out["source_sep"] == ","
        assert "__meta__" in out["source_schema"]

    def test_loads_gcs_uri(self):
        # Mock GCS loader
        fake_df = pd.DataFrame({"product_name": ["A", "B"], "brand_name": ["X", "Y"]})
        with (
            patch("src.pipeline.loaders.gcs_loader.is_gcs_uri", return_value=True),
            patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader") as mock_loader,
        ):
            mock_loader.return_value.load_sample.return_value = fake_df
            out = load_source_node({"source_path": "gs://bucket/path/*.jsonl"})

        assert "source_df" in out
        assert out["source_sep"] == ","

    def test_gcs_empty_raises(self):
        with (
            patch("src.pipeline.loaders.gcs_loader.is_gcs_uri", return_value=True),
            patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader") as mock_loader,
        ):
            mock_loader.return_value.load_sample.return_value = pd.DataFrame()
            with pytest.raises(ValueError, match="No data loaded"):
                load_source_node({"source_path": "gs://bucket/empty"})

    def test_detects_tab_separator(self, tmp_path):
        tsv = tmp_path / "data.tsv"
        tsv.write_text("name\tvalue\nA\t1\nB\t2\n")
        out = load_source_node({"source_path": str(tsv)})
        assert out["source_sep"] == "\t"

    def test_resets_llm_counter(self, tmp_path):
        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n")
        with patch("src.agents.orchestrator.reset_llm_counter") as mock_reset:
            load_source_node({"source_path": str(csv)})
            mock_reset.assert_called_once()


# ---------------------------------------------------------------------------
# check_registry_node (cache re-registration path)
# ---------------------------------------------------------------------------

class TestCheckRegistryNode:
    def test_short_circuits_when_already_run(self, tmp_path):
        from src.agents.orchestrator import check_registry_node
        yaml_path = tmp_path / "m.yaml"
        yaml_path.write_text("version: 1\noperations: []")
        state = {
            "block_registry_hits": {},
            "mapping_yaml_path": str(yaml_path),
            "domain": "nutrition",
        }
        with (
            patch("src.agents.orchestrator.DynamicMappingBlock") as mock_block,
            patch("src.agents.orchestrator.BlockRegistry") as mock_registry,
        ):
            out = check_registry_node(state)
        assert out == {}
        mock_registry.instance.return_value.register_block.assert_called_once()

    def test_short_circuit_missing_yaml_file(self):
        from src.agents.orchestrator import check_registry_node
        state = {
            "block_registry_hits": {},
            "mapping_yaml_path": "/nonexistent/path.yaml",
            "domain": "nutrition",
        }
        with patch("src.agents.orchestrator.BlockRegistry"):
            out = check_registry_node(state)
        # Still returns empty since already-ran flag present
        assert out == {}
