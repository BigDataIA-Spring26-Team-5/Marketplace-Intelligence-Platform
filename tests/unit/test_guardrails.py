"""Tests for src/agents/guardrails.py — input/output validators, HITL, clamping."""

from __future__ import annotations

import json

import pytest

from src.agents.guardrails import (
    CONFIDENCE_SCORE_MAX,
    CONFIDENCE_SCORE_MIN,
    DQ_SCORE_MAX,
    DQ_SCORE_MIN,
    HITL_LOW_CONFIDENCE_THRESHOLD,
    HITL_OPERATION_COUNT_THRESHOLD,
    HITL_UNRESOLVABLE_THRESHOLD,
    MAX_OPERATIONS_COUNT,
    MAX_RESPONSE_SIZE,
    RISK_SCORE_MAX,
    RISK_SCORE_MIN,
    SAFETY_COLUMNS,
    VALID_CATEGORIES,
    GuardrailAudit,
    GuardrailResult,
    HITLFlag,
    check_hitl_thresholds,
    check_json_parseable,
    check_no_prompt_leakage,
    check_response_size,
    clamp_value,
    run_guardrails_with_audit,
    run_input_guardrails,
    run_output_guardrails,
    validate_confidence_score,
    validate_critic_input,
    validate_critic_output,
    validate_dq_score,
    validate_enrichment_input,
    validate_enrichment_output,
    validate_risk_score,
    validate_schema_analysis_input,
    validate_schema_analysis_output,
    validate_sequence_planner_input,
    validate_sequence_planner_output,
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


class TestGuardrailResult:
    def test_passing_result_is_truthy(self):
        result = GuardrailResult(passed=True)
        assert bool(result) is True

    def test_failing_result_is_falsy(self):
        result = GuardrailResult(passed=False, errors=["bad"])
        assert bool(result) is False

    def test_default_lists_are_independent(self):
        r1 = GuardrailResult(passed=True)
        r2 = GuardrailResult(passed=True)
        r1.errors.append("x")
        assert r2.errors == []


class TestGuardrailAudit:
    def test_no_flags_means_no_review(self):
        audit = GuardrailAudit(call_site="schema_analysis")
        assert audit.requires_human_review is False

    def test_triggered_flag_requires_review(self):
        audit = GuardrailAudit(
            call_site="schema_analysis",
            hitl_flags=[HITLFlag(triggered=True, reason="test")],
        )
        assert audit.requires_human_review is True

    def test_untriggered_flag_does_not_require_review(self):
        audit = GuardrailAudit(
            call_site="schema_analysis",
            hitl_flags=[HITLFlag(triggered=False)],
        )
        assert audit.requires_human_review is False


# ---------------------------------------------------------------------------
# Input validators
# ---------------------------------------------------------------------------


class TestSchemaAnalysisInput:
    def test_valid_input_passes(self, sample_source_schema, sample_unified_schema):
        result = validate_schema_analysis_input(
            source_schema=sample_source_schema,
            unified_schema=sample_unified_schema,
        )
        assert result.passed is True
        assert result.errors == []

    def test_empty_source_schema_fails(self, sample_unified_schema):
        result = validate_schema_analysis_input({}, sample_unified_schema)
        assert result.passed is False
        assert any("source_schema is empty" in e for e in result.errors)

    def test_empty_unified_schema_fails(self, sample_source_schema):
        result = validate_schema_analysis_input(sample_source_schema, {})
        assert result.passed is False
        assert any("unified_schema is empty" in e for e in result.errors)

    def test_only_meta_columns_fails(self, sample_unified_schema):
        result = validate_schema_analysis_input(
            {"__meta__": {"row_count": 0}}, sample_unified_schema
        )
        assert result.passed is False

    def test_oversized_schema_warns(self, sample_unified_schema):
        huge_schema = {f"col_{i}": {"x": "y" * 200} for i in range(1000)}
        result = validate_schema_analysis_input(huge_schema, sample_unified_schema)
        assert any("very large" in w for w in result.warnings)


class TestCriticInput:
    def test_valid_input_passes(self, sample_source_schema, sample_unified_schema):
        result = validate_critic_input(
            column_mapping={"description": "product_name"},
            operations=[{"primitive": "RENAME", "source_column": "description"}],
            source_profile=sample_source_schema,
            unified_schema=sample_unified_schema,
        )
        assert result.passed is True

    def test_empty_mapping_and_operations_fails(self, sample_source_schema, sample_unified_schema):
        result = validate_critic_input({}, [], sample_source_schema, sample_unified_schema)
        assert result.passed is False

    def test_empty_source_profile_fails(self, sample_unified_schema):
        result = validate_critic_input(
            {"a": "b"}, [], {}, sample_unified_schema
        )
        assert result.passed is False

    def test_overlap_between_mapping_and_operations_warns(self, sample_source_schema, sample_unified_schema):
        result = validate_critic_input(
            column_mapping={"description": "product_name"},
            operations=[{"target_column": "product_name", "primitive": "ADD"}],
            source_profile=sample_source_schema,
            unified_schema=sample_unified_schema,
        )
        assert result.passed is True
        assert any("both column_mapping and operations" in w for w in result.warnings)


class TestSequencePlannerInput:
    def test_valid_input_passes(self):
        result = validate_sequence_planner_input(
            blocks_metadata=[{"name": "dq_score_pre"}], domain="nutrition"
        )
        assert result.passed is True

    def test_empty_blocks_fails(self):
        result = validate_sequence_planner_input([], "nutrition")
        assert result.passed is False

    def test_empty_domain_fails(self):
        result = validate_sequence_planner_input([{"name": "x"}], "")
        assert result.passed is False

    def test_whitespace_domain_fails(self):
        result = validate_sequence_planner_input([{"name": "x"}], "   ")
        assert result.passed is False


class TestEnrichmentInput:
    def test_valid_input_passes(self):
        rows = [{"product_name": "Cheerios"}]
        result = validate_enrichment_input(rows, batch_size=10)
        assert result.passed is True

    def test_empty_rows_fails(self):
        result = validate_enrichment_input([], batch_size=10)
        assert result.passed is False

    def test_oversized_batch_warns(self):
        rows = [{"product_name": "x"}]
        result = validate_enrichment_input(rows, batch_size=500)
        assert any("batch_size=500" in w for w in result.warnings)

    def test_missing_product_name_warns(self):
        rows = [{"description": "no name"}]
        result = validate_enrichment_input(rows, batch_size=10)
        assert any("product_name" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Output validators
# ---------------------------------------------------------------------------


class TestSchemaAnalysisOutput:
    @pytest.fixture
    def cols(self):
        return {
            "source_columns": {"description", "brand_owner", "ingredients"},
            "unified_columns": {
                "product_name", "brand_name", "ingredients",
                "primary_category", "allergens", "is_organic", "dietary_tags",
                "dq_score_pre", "dq_score_post",
            },
            "enrichment_columns": {"primary_category", "allergens", "is_organic", "dietary_tags"},
            "computed_columns": {"dq_score_pre", "dq_score_post"},
        }

    def test_clean_output_passes(self, cols):
        result = validate_schema_analysis_output(
            {
                "column_mapping": {"description": "product_name"},
                "operations": [
                    {"primitive": "RENAME", "source_column": "description", "target_column": "product_name"},
                ],
            },
            **cols,
        )
        assert result.passed is True

    def test_non_dict_response_fails(self, cols):
        result = validate_schema_analysis_output("not a dict", **cols)
        assert result.passed is False

    def test_hallucinated_source_column_fails(self, cols):
        result = validate_schema_analysis_output(
            {"column_mapping": {"made_up_col": "product_name"}, "operations": []},
            **cols,
        )
        assert result.passed is False
        assert any("hallucinated source column" in e for e in result.errors)

    def test_mapping_to_enrichment_column_fails(self, cols):
        result = validate_schema_analysis_output(
            {
                "column_mapping": {"description": "primary_category"},
                "operations": [],
            },
            **cols,
        )
        assert result.passed is False
        assert any("enrichment column" in e for e in result.errors)

    def test_mapping_to_computed_column_fails(self, cols):
        result = validate_schema_analysis_output(
            {
                "column_mapping": {"description": "dq_score_pre"},
                "operations": [],
            },
            **cols,
        )
        assert result.passed is False

    def test_safety_column_target_fails(self, cols):
        result = validate_schema_analysis_output(
            {
                "column_mapping": {},
                "operations": [
                    {"primitive": "ADD", "action": "set_default", "target_column": "allergens"},
                ],
            },
            **cols,
        )
        assert result.passed is False
        assert any("safety column" in e for e in result.errors)

    def test_safety_column_via_enrich_alias_allowed(self, cols):
        result = validate_schema_analysis_output(
            {
                "column_mapping": {},
                "operations": [
                    {"primitive": "ENRICH_ALIAS", "target_column": "allergens", "source_enrichment": "ext"},
                ],
            },
            **cols,
        )
        assert result.passed is True

    def test_invalid_primitive_fails(self, cols):
        result = validate_schema_analysis_output(
            {"column_mapping": {}, "operations": [{"primitive": "MAGIC"}]},
            **cols,
        )
        assert result.passed is False

    def test_invalid_add_action_fails(self, cols):
        result = validate_schema_analysis_output(
            {"column_mapping": {}, "operations": [
                {"primitive": "ADD", "action": "set_random", "target_column": "product_name"}]},
            **cols,
        )
        assert result.passed is False

    def test_runaway_operation_count_fails(self, cols):
        ops = [
            {"primitive": "ADD", "action": "set_null", "target_column": f"c{i}"}
            for i in range(MAX_OPERATIONS_COUNT + 1)
        ]
        result = validate_schema_analysis_output(
            {"column_mapping": {}, "operations": ops}, **cols,
        )
        assert result.passed is False
        assert any("max" in e for e in result.errors)

    def test_duplicate_operations_warn(self, cols):
        ops = [
            {"primitive": "ADD", "action": "set_null", "target_column": "product_name"},
            {"primitive": "ADD", "action": "set_null", "target_column": "product_name"},
        ]
        result = validate_schema_analysis_output(
            {"column_mapping": {}, "operations": ops}, **cols,
        )
        assert any("duplicate" in w for w in result.warnings)


class TestCriticOutput:
    def test_clean_output_passes(self):
        result = validate_critic_output(
            {"revised_operations": [{"primitive": "RENAME"}], "critique_notes": []},
            original_operations=[{"primitive": "RENAME"}],
        )
        assert result.passed is True

    def test_non_dict_response_fails(self):
        result = validate_critic_output("not a dict", [])
        assert result.passed is False

    def test_no_revised_uses_original(self):
        result = validate_critic_output({}, [{"primitive": "ADD"}])
        assert result.passed is True
        assert any("no revised_operations" in w for w in result.warnings)

    def test_revised_not_a_list_fails(self):
        result = validate_critic_output({"revised_operations": "bad"}, [])
        assert result.passed is False

    def test_excessive_additions_rejected(self):
        original = [{"primitive": "ADD"}]
        revised = [{"primitive": "ADD"} for _ in range(50)]
        result = validate_critic_output(
            {"revised_operations": revised, "critique_notes": []},
            original_operations=original,
        )
        assert result.passed is False
        assert any("hallucination" in e for e in result.errors)

    def test_invalid_primitive_in_revised_fails(self):
        result = validate_critic_output(
            {"revised_operations": [{"primitive": "EXTERMINATE"}]},
            original_operations=[],
        )
        assert result.passed is False

    def test_hallucinated_target_column_warns(self):
        result = validate_critic_output(
            {"revised_operations": [{"primitive": "RENAME", "target_column": "fake_col"}]},
            original_operations=[],
            unified_columns={"product_name", "brand_name"},
        )
        assert any("not in unified schema" in w for w in result.warnings)


class TestSequencePlannerOutput:
    def test_clean_output_passes(self):
        required = ["dq_score_pre", "fuzzy_deduplicate", "dq_score_post"]
        result = validate_sequence_planner_output(
            {"block_sequence": required}, required_blocks=required,
        )
        assert result.passed is True

    def test_non_dict_response_fails(self):
        result = validate_sequence_planner_output([], required_blocks=[])
        assert result.passed is False

    def test_sequence_not_a_list_fails(self):
        result = validate_sequence_planner_output(
            {"block_sequence": "x"}, required_blocks=[],
        )
        assert result.passed is False

    def test_unknown_block_fails(self):
        result = validate_sequence_planner_output(
            {"block_sequence": ["dq_score_pre", "magical_block", "dq_score_post"]},
            required_blocks=["dq_score_pre", "dq_score_post"],
        )
        assert result.passed is False
        assert any("Unknown blocks" in e for e in result.errors)

    def test_dq_score_pre_must_be_first(self):
        result = validate_sequence_planner_output(
            {"block_sequence": ["fuzzy_deduplicate", "dq_score_pre", "dq_score_post"]},
            required_blocks=["dq_score_pre", "fuzzy_deduplicate", "dq_score_post"],
        )
        assert result.passed is False

    def test_dq_score_post_must_be_last(self):
        result = validate_sequence_planner_output(
            {"block_sequence": ["dq_score_pre", "dq_score_post", "fuzzy_deduplicate"]},
            required_blocks=["dq_score_pre", "fuzzy_deduplicate", "dq_score_post"],
        )
        assert result.passed is False

    def test_dedup_before_normalization_fails(self):
        seq = ["dq_score_pre", "fuzzy_deduplicate", "strip_whitespace", "dq_score_post"]
        result = validate_sequence_planner_output(
            {"block_sequence": seq}, required_blocks=seq,
        )
        assert result.passed is False

    def test_extract_allergens_after_llm_enrich_fails(self):
        seq = ["dq_score_pre", "llm_enrich", "extract_allergens", "dq_score_post"]
        result = validate_sequence_planner_output(
            {"block_sequence": seq}, required_blocks=seq,
        )
        assert result.passed is False


class TestEnrichmentOutput:
    def test_valid_output_passes(self):
        result = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": "Snacks"}]},
            batch_size=1,
            batch_indices=[0],
        )
        assert result.passed is True

    def test_non_dict_response_fails(self):
        result = validate_enrichment_output("not a dict", 1, [0])
        assert result.passed is False

    def test_missing_results_key_fails(self):
        result = validate_enrichment_output({}, 1, [0])
        assert result.passed is False

    def test_safety_column_in_result_stripped_and_fails(self):
        result = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": "Snacks", "allergens": "milk"}]},
            batch_size=1,
            batch_indices=[0],
        )
        # An error is recorded; the field is stripped from the sanitized output.
        assert result.passed is False
        assert any("safety column" in e for e in result.errors)
        assert "allergens" not in result.sanitized_output["results"][0]

    def test_out_of_bounds_idx_warns_and_skips(self):
        result = validate_enrichment_output(
            {"results": [{"idx": 5, "primary_category": "Snacks"}]},
            batch_size=1,
            batch_indices=[0],
        )
        assert any("out of bounds" in w for w in result.warnings)
        assert result.sanitized_output["results"] == []

    def test_unknown_category_warns(self):
        result = validate_enrichment_output(
            {"results": [{"idx": 0, "primary_category": "Aliens"}]},
            batch_size=1,
            batch_indices=[0],
        )
        assert any("not in the" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Response-level checks
# ---------------------------------------------------------------------------


class TestResponseLevelChecks:
    def test_check_response_size_passes(self):
        assert check_response_size("hello").passed is True

    def test_check_response_size_fails(self):
        big = "x" * (MAX_RESPONSE_SIZE + 1)
        assert check_response_size(big).passed is False

    def test_check_json_parseable_valid(self):
        assert check_json_parseable('{"a": 1}').passed is True

    def test_check_json_parseable_markdown_fenced(self):
        result = check_json_parseable('```json\n{"a": 1}\n```')
        assert result.passed is True
        assert any("markdown fences" in w for w in result.warnings)

    def test_check_json_parseable_invalid(self):
        assert check_json_parseable("nope").passed is False

    def test_check_no_prompt_leakage_clean(self):
        assert check_no_prompt_leakage("regular response").warnings == []

    def test_check_no_prompt_leakage_detects_marker(self):
        result = check_no_prompt_leakage("You are a schema analysis agent ...")
        assert any("prompt fragment" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# HITL thresholds
# ---------------------------------------------------------------------------


class TestHITLThresholds:
    def test_no_flags_at_low_complexity(self):
        flags = check_hitl_thresholds(
            "schema_analysis",
            {"operations": [{}, {}], "unresolvable": []},
            confidence_scores=[0.95, 0.93],
        )
        assert flags == []

    def test_operation_count_threshold(self):
        ops = [{}] * (HITL_OPERATION_COUNT_THRESHOLD + 1)
        flags = check_hitl_thresholds(
            "schema_analysis", {"operations": ops, "unresolvable": []}
        )
        assert any(f.threshold_name == "operation_count" for f in flags)

    def test_unresolvable_threshold(self):
        unr = [{}] * (HITL_UNRESOLVABLE_THRESHOLD + 1)
        flags = check_hitl_thresholds(
            "schema_analysis", {"operations": [], "unresolvable": unr}
        )
        assert any(f.threshold_name == "unresolvable_count" for f in flags)

    def test_low_confidence_threshold(self):
        flags = check_hitl_thresholds(
            "schema_analysis",
            {"operations": [{}], "unresolvable": []},
            confidence_scores=[0.1, 0.2, 0.3],
        )
        assert any(f.threshold_name == "avg_confidence" for f in flags)

    def test_enrichment_large_batch_flag(self):
        flags = check_hitl_thresholds(
            "enrichment", {"results": [{} for _ in range(60)]}
        )
        assert any(f.threshold_name == "enrichment_batch_size" for f in flags)

    def test_unknown_call_site_no_flags(self):
        assert check_hitl_thresholds("does_not_exist", {}) == []


# ---------------------------------------------------------------------------
# Numerical clamping
# ---------------------------------------------------------------------------


class TestClamping:
    def test_clamp_in_range(self):
        assert clamp_value(0.5, 0.0, 1.0) == 0.5

    def test_clamp_below_min(self):
        assert clamp_value(-2.0, 0.0, 1.0) == 0.0

    def test_clamp_above_max(self):
        assert clamp_value(2.0, 0.0, 1.0) == 1.0

    def test_validate_confidence_clamps_high(self):
        assert validate_confidence_score(1.5) == CONFIDENCE_SCORE_MAX

    def test_validate_confidence_clamps_low(self):
        assert validate_confidence_score(-0.1) == CONFIDENCE_SCORE_MIN

    def test_validate_confidence_non_numeric_defaults_zero(self):
        assert validate_confidence_score("oops") == 0.0  # type: ignore[arg-type]

    def test_validate_dq_score_clamps_high(self):
        assert validate_dq_score(150.0) == DQ_SCORE_MAX

    def test_validate_dq_score_clamps_low(self):
        assert validate_dq_score(-10.0) == DQ_SCORE_MIN

    def test_validate_dq_score_preserves_decimals(self):
        # Decimal precision must be preserved across the clamp
        assert validate_dq_score(73.27) == 73.27
        assert validate_dq_score(0.001) == 0.001

    def test_validate_risk_score_clamps_high(self):
        assert validate_risk_score(99) == RISK_SCORE_MAX

    def test_validate_risk_score_clamps_low(self):
        assert validate_risk_score(-3) == RISK_SCORE_MIN

    def test_validate_risk_score_non_int_default(self):
        assert validate_risk_score(2.5) == 3  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Composite runners
# ---------------------------------------------------------------------------


class TestCompositeRunners:
    def test_run_input_guardrails_unknown_site_passes(self):
        result = run_input_guardrails("does_not_exist")
        assert result.passed is True

    def test_run_input_guardrails_routes_correctly(self, sample_source_schema, sample_unified_schema):
        result = run_input_guardrails(
            "schema_analysis",
            source_schema=sample_source_schema,
            unified_schema=sample_unified_schema,
        )
        assert result.passed is True

    def test_run_output_guardrails_size_short_circuits(self):
        result = run_output_guardrails(
            "schema_analysis",
            raw_response="x" * (MAX_RESPONSE_SIZE + 1),
            parsed_result={},
            source_columns=set(),
            unified_columns=set(),
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert result.passed is False

    def test_run_guardrails_with_audit_returns_tuple(self):
        result, audit = run_guardrails_with_audit(
            "sequence_planner",
            raw_response='{"block_sequence": ["dq_score_pre", "dq_score_post"]}',
            parsed_result={"block_sequence": ["dq_score_pre", "dq_score_post"]},
            required_blocks=["dq_score_pre", "dq_score_post"],
        )
        assert isinstance(result, GuardrailResult)
        assert isinstance(audit, GuardrailAudit)
        assert audit.elapsed_ms is not None and audit.elapsed_ms >= 0
        assert audit.timestamp is not None

    def test_run_guardrails_with_audit_emits_hitl_flag(self):
        big_ops = [
            {"primitive": "ADD", "action": "set_null", "target_column": f"c{i}"}
            for i in range(HITL_OPERATION_COUNT_THRESHOLD + 1)
        ]
        parsed = {"column_mapping": {}, "operations": big_ops, "unresolvable": []}
        _, audit = run_guardrails_with_audit(
            "schema_analysis",
            raw_response=json.dumps(parsed),
            parsed_result=parsed,
            source_columns=set(),
            unified_columns=set(),
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert audit.requires_human_review is True
