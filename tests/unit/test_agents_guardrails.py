"""Unit tests for src.agents.guardrails."""

from __future__ import annotations

import json

import pytest

from src.agents.guardrails import (
    GuardrailResult,
    GuardrailAudit,
    HITLFlag,
    VALID_PRIMITIVES,
    SAFETY_COLUMNS,
    validate_schema_analysis_input,
    validate_critic_input,
    validate_sequence_planner_input,
    validate_enrichment_input,
    validate_schema_analysis_output,
    validate_critic_output,
    validate_sequence_planner_output,
    validate_enrichment_output,
    check_response_size,
    check_json_parseable,
    check_no_prompt_leakage,
    run_input_guardrails,
    run_output_guardrails,
    check_hitl_thresholds,
    clamp_value,
    validate_confidence_score,
    validate_dq_score,
    validate_risk_score,
    run_guardrails_with_audit,
    MAX_RESPONSE_SIZE,
    MAX_OPERATIONS_COUNT,
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

class TestGuardrailResult:
    def test_bool_true_when_passed(self):
        r = GuardrailResult(passed=True)
        assert bool(r) is True

    def test_bool_false_when_failed(self):
        r = GuardrailResult(passed=False, errors=["bad"])
        assert bool(r) is False

    def test_default_errors_warnings_empty(self):
        r = GuardrailResult(passed=True)
        assert r.errors == []
        assert r.warnings == []


class TestHITLFlag:
    def test_defaults(self):
        f = HITLFlag(triggered=False)
        assert f.reason is None
        assert f.actual_value is None


class TestGuardrailAudit:
    def test_requires_review_false_when_no_flags(self):
        a = GuardrailAudit(call_site="x")
        assert a.requires_human_review is False

    def test_requires_review_true_when_flag_triggered(self):
        a = GuardrailAudit(
            call_site="x",
            hitl_flags=[HITLFlag(triggered=True, reason="big")],
        )
        assert a.requires_human_review is True

    def test_requires_review_false_when_flag_not_triggered(self):
        a = GuardrailAudit(
            call_site="x",
            hitl_flags=[HITLFlag(triggered=False)],
        )
        assert a.requires_human_review is False


# ---------------------------------------------------------------------------
# Input guardrails
# ---------------------------------------------------------------------------

class TestSchemaAnalysisInput:
    def test_passes_valid(self):
        r = validate_schema_analysis_input(
            source_schema={"col_a": {"dtype": "object"}},
            unified_schema={"product_name": {"type": "string"}},
        )
        assert r.passed

    def test_fails_empty_source(self):
        r = validate_schema_analysis_input(source_schema={}, unified_schema={"a": {}})
        assert not r.passed
        assert any("source_schema is empty" in e for e in r.errors)

    def test_fails_empty_unified(self):
        r = validate_schema_analysis_input(source_schema={"a": {}}, unified_schema={})
        assert not r.passed

    def test_fails_only_meta(self):
        r = validate_schema_analysis_input(source_schema={"__meta__": {}}, unified_schema={"a": {}})
        assert not r.passed

    def test_warns_large_schema(self):
        big = {f"col_{i}": {"data": "x" * 1000} for i in range(200)}
        r = validate_schema_analysis_input(source_schema=big, unified_schema={"a": {}})
        assert any("very large" in w for w in r.warnings)


class TestCriticInput:
    def test_passes_valid(self):
        r = validate_critic_input(
            column_mapping={"a": "b"},
            operations=[{"primitive": "RENAME"}],
            source_profile={"a": {}},
            unified_schema={"b": {}},
        )
        assert r.passed

    def test_fails_both_empty(self):
        r = validate_critic_input(
            column_mapping={}, operations=[],
            source_profile={"a": {}}, unified_schema={"b": {}},
        )
        assert not r.passed

    def test_fails_empty_source_profile(self):
        r = validate_critic_input(
            column_mapping={"a": "b"}, operations=[],
            source_profile={}, unified_schema={"b": {}},
        )
        assert not r.passed

    def test_warns_on_overlap(self):
        r = validate_critic_input(
            column_mapping={"src": "tgt"},
            operations=[{"primitive": "RENAME", "target_column": "tgt"}],
            source_profile={"src": {}},
            unified_schema={"tgt": {}},
        )
        assert r.passed
        assert any("both column_mapping and operations" in w for w in r.warnings)


class TestSequencePlannerInput:
    def test_passes_valid(self):
        r = validate_sequence_planner_input(blocks_metadata=[{"name": "b"}], domain="nutrition")
        assert r.passed

    def test_fails_empty_blocks(self):
        r = validate_sequence_planner_input(blocks_metadata=[], domain="nutrition")
        assert not r.passed

    def test_fails_empty_domain(self):
        r = validate_sequence_planner_input(blocks_metadata=[{"n": "x"}], domain="")
        assert not r.passed

    def test_fails_whitespace_domain(self):
        r = validate_sequence_planner_input(blocks_metadata=[{"n": "x"}], domain="   ")
        assert not r.passed


class TestEnrichmentInput:
    def test_passes_valid(self):
        r = validate_enrichment_input(rows=[{"product_name": "x"}], batch_size=50)
        assert r.passed

    def test_fails_empty_rows(self):
        r = validate_enrichment_input(rows=[], batch_size=50)
        assert not r.passed

    def test_warns_batch_size_too_large(self):
        r = validate_enrichment_input(rows=[{"product_name": "x"}], batch_size=500)
        assert r.passed
        assert r.warnings

    def test_warns_batch_size_too_small(self):
        r = validate_enrichment_input(rows=[{"product_name": "x"}], batch_size=0)
        assert r.warnings

    def test_warns_missing_product_name(self):
        r = validate_enrichment_input(rows=[{"other": "x"}], batch_size=50)
        assert any("product_name" in w for w in r.warnings)


# ---------------------------------------------------------------------------
# Output guardrails
# ---------------------------------------------------------------------------

class TestSchemaAnalysisOutput:
    def test_passes_valid(self):
        result = {
            "column_mapping": {"a": "b"},
            "operations": [{"primitive": "RENAME", "source_column": "a", "target_column": "b"}],
        }
        r = validate_schema_analysis_output(
            result,
            source_columns={"a"},
            unified_columns={"b"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert r.passed

    def test_fails_non_dict(self):
        r = validate_schema_analysis_output(
            "not a dict", source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_fails_hallucinated_source(self):
        result = {"column_mapping": {"ghost": "b"}, "operations": []}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"b"},
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed
        assert any("hallucinated" in e for e in r.errors)

    def test_fails_map_to_enrichment(self):
        result = {"column_mapping": {"a": "primary_category"}, "operations": []}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"primary_category"},
            enrichment_columns={"primary_category"}, computed_columns=set(),
        )
        assert not r.passed

    def test_fails_map_to_computed(self):
        result = {"column_mapping": {"a": "dq_score"}, "operations": []}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"dq_score"},
            enrichment_columns=set(), computed_columns={"dq_score"},
        )
        assert not r.passed

    def test_fails_invalid_primitive(self):
        result = {"column_mapping": {}, "operations": [{"primitive": "BOGUS"}]}
        r = validate_schema_analysis_output(
            result, source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_fails_too_many_operations(self):
        ops = [{"primitive": "RENAME", "target_column": f"c{i}"} for i in range(MAX_OPERATIONS_COUNT + 1)]
        result = {"column_mapping": {}, "operations": ops}
        r = validate_schema_analysis_output(
            result, source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_fails_hallucinated_op_source(self):
        result = {"column_mapping": {}, "operations": [
            {"primitive": "RENAME", "source_column": "ghost", "target_column": "b"}
        ]}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"b"},
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_fails_targeting_safety_column(self):
        result = {"column_mapping": {}, "operations": [
            {"primitive": "RENAME", "source_column": "a", "target_column": "allergens"}
        ]}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"allergens"},
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_enrich_alias_allowed_on_safety(self):
        result = {"column_mapping": {}, "operations": [
            {"primitive": "ENRICH_ALIAS", "target_column": "allergens"}
        ]}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"allergens"},
            enrichment_columns=set(), computed_columns=set(),
        )
        # ENRICH_ALIAS is permitted for safety cols
        assert r.passed

    def test_fails_invalid_add_action(self):
        result = {"column_mapping": {}, "operations": [
            {"primitive": "ADD", "action": "bogus", "target_column": "x"}
        ]}
        r = validate_schema_analysis_output(
            result, source_columns=set(), unified_columns={"x"},
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_warns_duplicate_ops(self):
        result = {"column_mapping": {}, "operations": [
            {"primitive": "RENAME", "source_column": "a", "target_column": "b"},
            {"primitive": "RENAME", "source_column": "a", "target_column": "b"},
        ]}
        r = validate_schema_analysis_output(
            result, source_columns={"a"}, unified_columns={"b"},
            enrichment_columns=set(), computed_columns=set(),
        )
        assert r.warnings


class TestCriticOutput:
    def test_passes_valid(self):
        result = {
            "revised_operations": [{"primitive": "RENAME", "target_column": "b"}],
            "critique_notes": [{"rule": "R1"}],
        }
        r = validate_critic_output(result, original_operations=[{"primitive": "RENAME"}])
        assert r.passed

    def test_fails_non_dict(self):
        r = validate_critic_output("x", original_operations=[])
        assert not r.passed

    def test_warns_no_revised(self):
        r = validate_critic_output({}, original_operations=[])
        assert r.passed
        assert r.warnings

    def test_fails_revised_not_list(self):
        r = validate_critic_output({"revised_operations": "nope"}, original_operations=[])
        assert not r.passed

    def test_fails_explosion(self):
        orig = [{"primitive": "RENAME"}]
        revised = [{"primitive": "RENAME"}] * 100
        r = validate_critic_output({"revised_operations": revised}, original_operations=orig)
        assert not r.passed

    def test_fails_invalid_primitive_in_revised(self):
        r = validate_critic_output(
            {"revised_operations": [{"primitive": "BOGUS"}]},
            original_operations=[],
        )
        assert not r.passed

    def test_warns_on_hallucinated_target(self):
        r = validate_critic_output(
            {"revised_operations": [{"primitive": "RENAME", "target_column": "ghost"}]},
            original_operations=[],
            unified_columns={"b"},
        )
        assert r.warnings


class TestSequencePlannerOutput:
    def test_passes_valid(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["dq_score_pre", "strip_whitespace", "fuzzy_deduplicate", "dq_score_post"]},
            required_blocks=["dq_score_pre", "strip_whitespace", "fuzzy_deduplicate", "dq_score_post"],
        )
        assert r.passed

    def test_fails_non_dict(self):
        r = validate_sequence_planner_output("x", required_blocks=[])
        assert not r.passed

    def test_fails_sequence_not_list(self):
        r = validate_sequence_planner_output({"block_sequence": "not a list"}, required_blocks=[])
        assert not r.passed

    def test_warns_missing_blocks(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["a"]}, required_blocks=["a", "b"],
        )
        assert r.warnings

    def test_fails_unknown_blocks(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["a", "unknown_block"]}, required_blocks=["a"],
        )
        assert not r.passed

    def test_fails_dq_pre_not_first(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["x", "dq_score_pre"]}, required_blocks=["x", "dq_score_pre"],
        )
        assert not r.passed

    def test_fails_dq_post_not_last(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["dq_score_post", "x"]},
            required_blocks=["dq_score_post", "x"],
        )
        assert not r.passed

    def test_fails_norm_after_dedup(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["fuzzy_deduplicate", "strip_whitespace"]},
            required_blocks=["fuzzy_deduplicate", "strip_whitespace"],
        )
        assert not r.passed

    def test_fails_allergens_after_enrich(self):
        r = validate_sequence_planner_output(
            {"block_sequence": ["llm_enrich", "extract_allergens"]},
            required_blocks=["llm_enrich", "extract_allergens"],
        )
        assert not r.passed


class TestEnrichmentOutput:
    def test_passes_valid(self):
        result = {"results": [{"idx": 0, "primary_category": "Dairy"}]}
        r = validate_enrichment_output(result, batch_size=1, batch_indices=[0])
        assert r.passed

    def test_fails_non_dict(self):
        r = validate_enrichment_output("x", batch_size=1, batch_indices=[0])
        assert not r.passed

    def test_fails_no_results_key(self):
        r = validate_enrichment_output({}, batch_size=1, batch_indices=[0])
        assert not r.passed

    def test_warns_item_non_dict(self):
        r = validate_enrichment_output({"results": ["x"]}, batch_size=1, batch_indices=[0])
        assert r.warnings

    def test_warns_invalid_idx(self):
        r = validate_enrichment_output(
            {"results": [{"idx": "not int", "primary_category": "X"}]},
            batch_size=1, batch_indices=[0],
        )
        assert r.warnings

    def test_warns_out_of_bounds(self):
        r = validate_enrichment_output(
            {"results": [{"idx": 99, "primary_category": "Dairy"}]},
            batch_size=1, batch_indices=[0],
        )
        assert r.warnings

    def test_warns_non_string_category(self):
        r = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": 42}]},
            batch_size=1, batch_indices=[0],
        )
        assert r.warnings

    def test_warns_unknown_category(self):
        r = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": "Martian"}]},
            batch_size=1, batch_indices=[0],
        )
        assert r.warnings

    def test_fails_safety_column_inferred(self):
        r = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": "Dairy", "allergens": "milk"}]},
            batch_size=1, batch_indices=[0],
        )
        assert not r.passed
        # safety col stripped in sanitized
        assert "allergens" not in r.sanitized_output["results"][0]


# ---------------------------------------------------------------------------
# Response-level checks
# ---------------------------------------------------------------------------

class TestResponseChecks:
    def test_size_ok(self):
        assert check_response_size("short").passed

    def test_size_too_big(self):
        big = "x" * (MAX_RESPONSE_SIZE + 1)
        assert not check_response_size(big).passed

    def test_json_parseable_ok(self):
        assert check_json_parseable('{"a": 1}').passed

    def test_json_parseable_fail(self):
        assert not check_json_parseable("not json").passed

    def test_json_parseable_markdown_fence(self):
        r = check_json_parseable('```json\n{"a": 1}\n```')
        assert r.passed
        assert r.warnings

    def test_json_parseable_bad_fence(self):
        r = check_json_parseable('```json\nnope\n```')
        assert not r.passed

    def test_prompt_leakage_clean(self):
        r = check_no_prompt_leakage("Just a normal response")
        assert r.passed
        assert not r.warnings

    def test_prompt_leakage_detected(self):
        r = check_no_prompt_leakage("You are a schema analysis agent ...")
        assert r.passed  # warning only
        assert r.warnings


# ---------------------------------------------------------------------------
# Composite runners
# ---------------------------------------------------------------------------

class TestRunInputGuardrails:
    def test_valid_schema_analysis(self):
        r = run_input_guardrails(
            "schema_analysis",
            source_schema={"a": {}}, unified_schema={"b": {}},
        )
        assert r.passed

    def test_unknown_call_site(self):
        r = run_input_guardrails("bogus")
        assert r.passed  # returns passed=True with warning

    def test_blocked_on_error(self):
        r = run_input_guardrails("schema_analysis", source_schema={}, unified_schema={})
        assert not r.passed


class TestRunOutputGuardrails:
    def test_valid(self):
        r = run_output_guardrails(
            "schema_analysis",
            raw_response='{"column_mapping": {}, "operations": []}',
            parsed_result={"column_mapping": {}, "operations": []},
            source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert r.passed

    def test_blocked_on_size(self):
        big = "x" * (MAX_RESPONSE_SIZE + 1)
        r = run_output_guardrails(
            "schema_analysis", raw_response=big, parsed_result={},
            source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert not r.passed

    def test_unknown_call_site_passes(self):
        r = run_output_guardrails("bogus", raw_response="{}", parsed_result={})
        assert r.passed


# ---------------------------------------------------------------------------
# HITL thresholds
# ---------------------------------------------------------------------------

class TestHITLThresholds:
    def test_no_flags_when_small(self):
        flags = check_hitl_thresholds("schema_analysis", {"operations": [{"p": 1}], "unresolvable": []})
        assert flags == []

    def test_flags_on_many_operations(self):
        ops = [{"p": i} for i in range(20)]
        flags = check_hitl_thresholds("schema_analysis", {"operations": ops, "unresolvable": []})
        assert any(f.threshold_name == "operation_count" for f in flags)

    def test_flags_on_many_unresolvable(self):
        un = [{"c": i} for i in range(10)]
        flags = check_hitl_thresholds("schema_analysis", {"operations": [], "unresolvable": un})
        assert any(f.threshold_name == "unresolvable_count" for f in flags)

    def test_flags_on_low_confidence(self):
        flags = check_hitl_thresholds(
            "schema_analysis",
            {"operations": [], "unresolvable": []},
            confidence_scores=[0.1, 0.2, 0.3],
        )
        assert any(f.threshold_name == "avg_confidence" for f in flags)

    def test_enrichment_large_batch(self):
        results = [{"idx": i} for i in range(60)]
        flags = check_hitl_thresholds("enrichment", {"results": results})
        assert any(f.threshold_name == "enrichment_batch_size" for f in flags)

    def test_unknown_call_site_no_flags(self):
        assert check_hitl_thresholds("bogus", {}) == []


# ---------------------------------------------------------------------------
# Clamping utilities
# ---------------------------------------------------------------------------

class TestClamping:
    def test_clamp_within(self):
        assert clamp_value(0.5, 0.0, 1.0) == 0.5

    def test_clamp_low(self):
        assert clamp_value(-1.0, 0.0, 1.0) == 0.0

    def test_clamp_high(self):
        assert clamp_value(5.0, 0.0, 1.0) == 1.0

    def test_validate_confidence_score_valid(self):
        assert validate_confidence_score(0.7) == 0.7

    def test_validate_confidence_score_clamped(self):
        assert validate_confidence_score(2.0) == 1.0
        assert validate_confidence_score(-0.5) == 0.0

    def test_validate_confidence_score_non_numeric(self):
        assert validate_confidence_score("hi") == 0.0

    def test_validate_dq_score_valid(self):
        assert validate_dq_score(50.0) == 50.0

    def test_validate_dq_score_clamped(self):
        assert validate_dq_score(150.0) == 100.0

    def test_validate_dq_score_non_numeric(self):
        assert validate_dq_score(None) == 0.0

    def test_validate_risk_score_valid(self):
        assert validate_risk_score(3) == 3

    def test_validate_risk_score_clamped(self):
        assert validate_risk_score(99) == 5
        assert validate_risk_score(0) == 1

    def test_validate_risk_score_non_int(self):
        assert validate_risk_score(3.5) == 3  # defaults to 3


# ---------------------------------------------------------------------------
# Full pipeline with audit
# ---------------------------------------------------------------------------

class TestRunGuardrailsWithAudit:
    def test_returns_result_and_audit(self):
        parsed = {"column_mapping": {}, "operations": []}
        r, audit = run_guardrails_with_audit(
            "schema_analysis",
            raw_response=json.dumps(parsed),
            parsed_result=parsed,
            model_version="test-v1",
            source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert r.passed
        assert audit.call_site == "schema_analysis"
        assert audit.model_version == "test-v1"
        assert audit.elapsed_ms is not None
        assert audit.timestamp is not None

    def test_audit_captures_hitl_flag(self):
        ops = [{"primitive": "RENAME", "source_column": f"c{i}", "target_column": f"t{i}"} for i in range(20)]
        parsed = {"column_mapping": {}, "operations": ops, "unresolvable": []}
        source_cols = {f"c{i}" for i in range(20)}
        unified_cols = {f"t{i}" for i in range(20)}
        _, audit = run_guardrails_with_audit(
            "schema_analysis",
            raw_response="{}",
            parsed_result=parsed,
            source_columns=source_cols, unified_columns=unified_cols,
            enrichment_columns=set(), computed_columns=set(),
        )
        assert audit.requires_human_review

    def test_audit_with_confidence_scores(self):
        parsed = {"column_mapping": {}, "operations": [], "unresolvable": []}
        _, audit = run_guardrails_with_audit(
            "schema_analysis",
            raw_response="{}",
            parsed_result=parsed,
            confidence_scores=[0.9, 0.8],
            source_columns=set(), unified_columns=set(),
            enrichment_columns=set(), computed_columns=set(),
        )
        assert "confidence_threshold" in audit.checks_performed
