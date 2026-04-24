"""Unit tests for Pydantic models (src/schema/models.py) and agent guardrails/validation."""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest
from pydantic import ValidationError

from src.schema.models import ColumnSpec, DQWeights, UnifiedSchema
from src.agents.state import PipelineState
from src.agents.guardrails import (
    GuardrailResult,
    validate_schema_analysis_input,
    validate_schema_analysis_output,
    validate_critic_input,
    validate_critic_output,
    validate_sequence_planner_input,
    validate_sequence_planner_output,
    validate_enrichment_input,
    validate_enrichment_output,
    check_response_size,
    check_json_parseable,
    check_no_prompt_leakage,
    run_input_guardrails,
    run_output_guardrails,
    VALID_PRIMITIVES,
    SAFETY_COLUMNS,
)


# ============================================================================
# SECTION 1: Pydantic Model Tests — ColumnSpec
# ============================================================================


class TestColumnSpec:
    """Tests for the ColumnSpec model."""

    def test_valid_string_column(self):
        col = ColumnSpec(type="string", required=True)
        assert col.type == "string"
        assert col.required is True
        assert col.enrichment is False
        assert col.computed is False
        assert col.enrichment_alias is None

    def test_valid_float_column(self):
        col = ColumnSpec(type="float", required=False, enrichment=True)
        assert col.type == "float"
        assert col.enrichment is True

    def test_valid_integer_column(self):
        col = ColumnSpec(type="integer")
        assert col.type == "integer"

    def test_valid_boolean_column(self):
        col = ColumnSpec(type="boolean", computed=True)
        assert col.type == "boolean"
        assert col.computed is True

    def test_invalid_type_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ColumnSpec(type="varchar")

    def test_invalid_type_number(self):
        with pytest.raises(ValidationError):
            ColumnSpec(type="number")

    def test_enrichment_alias_field(self):
        col = ColumnSpec(type="string", required=True, enrichment_alias="primary_category")
        assert col.enrichment_alias == "primary_category"

    def test_extra_fields_allowed(self):
        """ColumnSpec has extra='allow' — future flags should not break validation."""
        col = ColumnSpec(type="string", future_flag=True)
        assert col.type == "string"

    def test_defaults(self):
        col = ColumnSpec(type="string")
        assert col.required is False
        assert col.enrichment is False
        assert col.computed is False
        assert col.enrichment_alias is None


# ============================================================================
# SECTION 2: Pydantic Model Tests — DQWeights
# ============================================================================


class TestDQWeights:
    """Tests for the DQWeights model and its sum-to-one validator."""

    def test_default_weights_sum_to_one(self):
        w = DQWeights()
        assert w.completeness == 0.4
        assert w.freshness == 0.35
        assert w.ingredient_richness == 0.25
        assert abs(w.completeness + w.freshness + w.ingredient_richness - 1.0) < 1e-6

    def test_custom_valid_weights(self):
        w = DQWeights(completeness=0.5, freshness=0.3, ingredient_richness=0.2)
        assert w.completeness == 0.5

    def test_weights_not_summing_to_one_raises(self):
        with pytest.raises(ValidationError, match="must sum to 1.0"):
            DQWeights(completeness=0.5, freshness=0.5, ingredient_richness=0.5)

    def test_weights_slightly_off_raises(self):
        with pytest.raises(ValidationError, match="must sum to 1.0"):
            DQWeights(completeness=0.4, freshness=0.35, ingredient_richness=0.26)

    def test_zero_weights_raises(self):
        with pytest.raises(ValidationError, match="must sum to 1.0"):
            DQWeights(completeness=0.0, freshness=0.0, ingredient_richness=0.0)

    def test_extra_fields_allowed(self):
        w = DQWeights(completeness=0.4, freshness=0.35, ingredient_richness=0.25, new_dim=0.0)
        assert w.completeness == 0.4


# ============================================================================
# SECTION 3: Pydantic Model Tests — UnifiedSchema
# ============================================================================


class TestUnifiedSchema:
    """Tests for the UnifiedSchema model and its computed properties."""

    @pytest.fixture
    def sample_schema(self):
        return UnifiedSchema(
            columns={
                "product_name": ColumnSpec(type="string", required=True),
                "brand_name": ColumnSpec(type="string", required=True),
                "category": ColumnSpec(type="string", required=True, enrichment_alias="primary_category"),
                "primary_category": ColumnSpec(type="string", enrichment=True),
                "allergens": ColumnSpec(type="string", enrichment=True),
                "dq_score_pre": ColumnSpec(type="float", computed=True),
                "dq_score_post": ColumnSpec(type="float", computed=True),
            },
            dq_weights=DQWeights(),
        )

    def test_mappable_columns_excludes_computed_and_enrichment(self, sample_schema):
        mappable = sample_schema.mappable_columns
        assert "product_name" in mappable
        assert "brand_name" in mappable
        assert "category" in mappable
        # Enrichment and computed should be excluded
        assert "primary_category" not in mappable
        assert "allergens" not in mappable
        assert "dq_score_pre" not in mappable
        assert "dq_score_post" not in mappable

    def test_required_columns(self, sample_schema):
        required = sample_schema.required_columns
        assert "product_name" in required
        assert "brand_name" in required
        assert "category" in required
        # Computed cols are excluded even if required
        assert "dq_score_pre" not in required

    def test_enrichment_columns(self, sample_schema):
        enrichment = sample_schema.enrichment_columns
        assert "primary_category" in enrichment
        assert "allergens" in enrichment
        assert "product_name" not in enrichment

    def test_for_prompt_excludes_computed(self, sample_schema):
        prompt_dict = sample_schema.for_prompt()
        assert "dq_score_pre" not in prompt_dict["columns"]
        assert "dq_score_post" not in prompt_dict["columns"]
        assert "product_name" in prompt_dict["columns"]
        assert "primary_category" in prompt_dict["columns"]

    def test_to_json_roundtrip(self, sample_schema):
        json_str = sample_schema.to_json(indent=2)
        data = json.loads(json_str)
        assert "columns" in data
        assert "dq_weights" in data
        # Reconstruct from JSON
        reconstructed = UnifiedSchema(**data)
        assert reconstructed.columns.keys() == sample_schema.columns.keys()

    def test_empty_columns_valid(self):
        schema = UnifiedSchema(columns={})
        assert schema.mappable_columns == {}
        assert schema.required_columns == set()

    def test_missing_columns_field_raises(self):
        with pytest.raises(ValidationError):
            UnifiedSchema()


# ============================================================================
# SECTION 4: Agent State Tests
# ============================================================================


class TestPipelineState:
    """Tests for PipelineState TypedDict structure."""

    def test_minimal_state(self):
        """PipelineState with total=False allows partial state."""
        state: PipelineState = {"source_path": "/data/test.csv"}
        assert state["source_path"] == "/data/test.csv"

    def test_state_with_multiple_fields(self):
        state: PipelineState = {
            "source_path": "/data/test.csv",
            "domain": "nutrition",
            "enable_enrichment": True,
            "column_mapping": {"description": "product_name"},
            "operations": [{"primitive": "RENAME"}],
        }
        assert state["domain"] == "nutrition"
        assert len(state["operations"]) == 1

    def test_state_fields_can_be_absent(self):
        """total=False means keys are optional at runtime."""
        state: PipelineState = {}
        assert state.get("source_df") is None
        assert state.get("operations") is None


# ============================================================================
# SECTION 5: Guardrails — Input Validation
# ============================================================================


class TestInputGuardrails:
    """Tests for input validation guardrails before LLM calls."""

    # --- Schema Analysis Input ---

    def test_schema_analysis_valid_input(self):
        result = validate_schema_analysis_input(
            source_schema={"col1": {"dtype": "string"}, "col2": {"dtype": "int64"}},
            unified_schema={"columns": {"product_name": {"type": "string"}}},
        )
        assert result.passed

    def test_schema_analysis_empty_source(self):
        result = validate_schema_analysis_input(
            source_schema={},
            unified_schema={"columns": {"product_name": {"type": "string"}}},
        )
        assert not result.passed
        assert any("empty" in e for e in result.errors)

    def test_schema_analysis_empty_unified(self):
        result = validate_schema_analysis_input(
            source_schema={"col1": {"dtype": "string"}},
            unified_schema={},
        )
        assert not result.passed

    def test_schema_analysis_only_meta(self):
        result = validate_schema_analysis_input(
            source_schema={"__meta__": {"sampling": {}}},
            unified_schema={"columns": {}},
        )
        assert not result.passed
        assert any("only __meta__" in e for e in result.errors)

    def test_schema_analysis_large_payload_warns(self):
        # Create a source schema larger than 100K chars
        large_schema = {f"col_{i}": {"dtype": "string", "samples": "x" * 1000} for i in range(200)}
        result = validate_schema_analysis_input(
            source_schema=large_schema,
            unified_schema={"columns": {"a": {}}},
        )
        assert result.passed  # passes but warns
        assert len(result.warnings) > 0

    # --- Critic Input ---

    def test_critic_valid_input(self):
        result = validate_critic_input(
            column_mapping={"desc": "product_name"},
            operations=[{"primitive": "ADD", "target_column": "brand_name"}],
            source_profile={"desc": {"dtype": "string"}},
            unified_schema={"columns": {}},
        )
        assert result.passed

    def test_critic_both_empty(self):
        result = validate_critic_input(
            column_mapping={},
            operations=[],
            source_profile={"col": {}},
            unified_schema={"columns": {}},
        )
        assert not result.passed

    def test_critic_empty_profile(self):
        result = validate_critic_input(
            column_mapping={"a": "b"},
            operations=[],
            source_profile={},
            unified_schema={"columns": {}},
        )
        assert not result.passed

    def test_critic_overlap_warning(self):
        result = validate_critic_input(
            column_mapping={"desc": "product_name"},
            operations=[{"primitive": "FORMAT", "target_column": "product_name"}],
            source_profile={"desc": {"dtype": "string"}},
            unified_schema={"columns": {}},
        )
        assert result.passed
        assert any("product_name" in w for w in result.warnings)

    # --- Sequence Planner Input ---

    def test_sequence_planner_valid_input(self):
        result = validate_sequence_planner_input(
            blocks_metadata=[{"name": "dq_score_pre"}],
            domain="nutrition",
        )
        assert result.passed

    def test_sequence_planner_empty_blocks(self):
        result = validate_sequence_planner_input(blocks_metadata=[], domain="nutrition")
        assert not result.passed

    def test_sequence_planner_empty_domain(self):
        result = validate_sequence_planner_input(
            blocks_metadata=[{"name": "x"}], domain=""
        )
        assert not result.passed

    # --- Enrichment Input ---

    def test_enrichment_valid_input(self):
        result = validate_enrichment_input(
            rows=[{"product_name": "Oat Milk", "brand_name": "Oatly"}],
            batch_size=20,
        )
        assert result.passed

    def test_enrichment_empty_rows(self):
        result = validate_enrichment_input(rows=[], batch_size=20)
        assert not result.passed

    def test_enrichment_extreme_batch_size_warns(self):
        result = validate_enrichment_input(
            rows=[{"product_name": "Test"}],
            batch_size=200,
        )
        assert result.passed
        assert len(result.warnings) > 0

    def test_enrichment_missing_product_name_warns(self):
        result = validate_enrichment_input(
            rows=[{"brand": "X"}],
            batch_size=10,
        )
        assert result.passed
        assert any("product_name" in w for w in result.warnings)


# ============================================================================
# SECTION 6: Guardrails — Output Validation
# ============================================================================


class TestOutputGuardrails:
    """Tests for output validation guardrails on LLM responses."""

    # --- Schema Analysis Output ---

    def test_schema_analysis_output_valid(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {"description": "product_name"},
                "operations": [
                    {"primitive": "ADD", "target_column": "brand_name", "action": "set_null"}
                ],
                "unresolvable": [],
            },
            source_columns={"description", "brand"},
            unified_columns={"product_name", "brand_name"},
            enrichment_columns={"primary_category"},
            computed_columns={"dq_score_pre"},
        )
        assert result.passed

    def test_schema_analysis_output_not_dict(self):
        result = validate_schema_analysis_output(
            result="not a dict",
            source_columns=set(),
            unified_columns=set(),
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed

    def test_schema_analysis_output_invalid_primitive(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [{"primitive": "TRANSFORM", "target_column": "x"}],
            },
            source_columns={"a"},
            unified_columns={"x"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed
        assert any("invalid primitive" in e for e in result.errors)

    def test_schema_analysis_output_mapping_to_enrichment_blocked(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {"cat_col": "primary_category"},
                "operations": [],
            },
            source_columns={"cat_col"},
            unified_columns={"primary_category"},
            enrichment_columns={"primary_category"},
            computed_columns=set(),
        )
        assert not result.passed
        assert any("enrichment column" in e for e in result.errors)

    def test_schema_analysis_output_mapping_to_computed_blocked(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {"score": "dq_score_pre"},
                "operations": [],
            },
            source_columns={"score"},
            unified_columns={"dq_score_pre"},
            enrichment_columns=set(),
            computed_columns={"dq_score_pre"},
        )
        assert not result.passed
        assert any("computed column" in e for e in result.errors)

    def test_schema_analysis_output_safety_column_violation(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {"primitive": "DERIVE", "target_column": "allergens", "action": "expression"}
                ],
            },
            source_columns={"ingredients"},
            unified_columns={"allergens"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed
        assert any("safety column" in e for e in result.errors)

    def test_schema_analysis_output_invalid_add_action(self):
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {"primitive": "ADD", "target_column": "x", "action": "invent_data"}
                ],
            },
            source_columns=set(),
            unified_columns={"x"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed
        assert any("ADD with invalid action" in e for e in result.errors)

    # --- Critic Output ---

    def test_critic_output_valid(self):
        result = validate_critic_output(
            result={
                "revised_operations": [
                    {"primitive": "ADD", "target_column": "x", "action": "set_null"}
                ],
                "critique_notes": [
                    {"rule": "Rule 1", "column": "x", "correction": "fixed"}
                ],
            },
            original_operations=[{"primitive": "ADD", "target_column": "x"}],
        )
        assert result.passed

    def test_critic_output_no_revised_ops_uses_original(self):
        result = validate_critic_output(
            result={"critique_notes": []},
            original_operations=[{"primitive": "ADD"}],
        )
        assert result.passed
        assert any("no revised_operations" in w for w in result.warnings)

    def test_critic_output_hallucination_detection(self):
        """If critic returns 3x more operations than input, flag as hallucination."""
        original = [{"primitive": "ADD"} for _ in range(5)]
        hallucinated = [{"primitive": "ADD"} for _ in range(50)]
        result = validate_critic_output(
            result={"revised_operations": hallucinated, "critique_notes": []},
            original_operations=original,
        )
        assert not result.passed
        assert any("hallucination" in e for e in result.errors)

    def test_critic_output_invalid_primitive_in_revised(self):
        result = validate_critic_output(
            result={
                "revised_operations": [{"primitive": "MAGIC"}],
                "critique_notes": [],
            },
            original_operations=[{"primitive": "ADD"}],
        )
        assert not result.passed

    # --- Sequence Planner Output ---

    def test_sequence_planner_output_valid(self):
        blocks = ["dq_score_pre", "strip_whitespace", "fuzzy_deduplicate", "dq_score_post"]
        result = validate_sequence_planner_output(
            result={"block_sequence": blocks, "reasoning": "standard order"},
            required_blocks=blocks,
        )
        assert result.passed

    def test_sequence_planner_output_wrong_first(self):
        blocks = ["strip_whitespace", "dq_score_pre", "dq_score_post"]
        result = validate_sequence_planner_output(
            result={"block_sequence": blocks},
            required_blocks=blocks,
        )
        assert not result.passed
        assert any("dq_score_pre must be first" in e for e in result.errors)

    def test_sequence_planner_output_wrong_last(self):
        blocks = ["dq_score_pre", "dq_score_post", "strip_whitespace"]
        result = validate_sequence_planner_output(
            result={"block_sequence": blocks},
            required_blocks=blocks,
        )
        assert not result.passed
        assert any("dq_score_post must be last" in e for e in result.errors)

    def test_sequence_planner_output_hallucinated_block(self):
        result = validate_sequence_planner_output(
            result={"block_sequence": ["dq_score_pre", "magic_block", "dq_score_post"]},
            required_blocks=["dq_score_pre", "dq_score_post"],
        )
        assert not result.passed
        assert any("hallucinated" in e for e in result.errors)

    def test_sequence_planner_output_missing_block_warns(self):
        result = validate_sequence_planner_output(
            result={"block_sequence": ["dq_score_pre", "dq_score_post"]},
            required_blocks=["dq_score_pre", "strip_whitespace", "dq_score_post"],
        )
        # Missing blocks are a warning (they get appended), not an error
        assert any("missing" in w.lower() for w in result.warnings)

    def test_sequence_planner_norm_before_dedup(self):
        blocks = [
            "dq_score_pre", "fuzzy_deduplicate", "strip_whitespace", "dq_score_post"
        ]
        result = validate_sequence_planner_output(
            result={"block_sequence": blocks},
            required_blocks=blocks,
        )
        assert not result.passed
        assert any("Normalization" in e for e in result.errors)

    def test_sequence_planner_allergens_before_enrich(self):
        blocks = [
            "dq_score_pre", "llm_enrich", "nutrition__extract_allergens", "dq_score_post"
        ]
        result = validate_sequence_planner_output(
            result={"block_sequence": blocks},
            required_blocks=blocks,
        )
        assert not result.passed
        assert any("extract_allergens" in e for e in result.errors)

    # --- Enrichment Output ---

    def test_enrichment_output_valid(self):
        result = validate_enrichment_output(
            result={"results": [{"idx": 0, "primary_category": "Dairy"}]},
            batch_size=5,
            batch_indices=[10, 11, 12, 13, 14],
        )
        assert result.passed

    def test_enrichment_output_not_dict(self):
        result = validate_enrichment_output(
            result="bad", batch_size=5, batch_indices=[0]
        )
        assert not result.passed

    def test_enrichment_output_idx_out_of_bounds(self):
        result = validate_enrichment_output(
            result={"results": [{"idx": 99, "primary_category": "Snacks"}]},
            batch_size=5,
            batch_indices=[0, 1, 2],
        )
        assert result.passed  # out-of-bounds items are warned and skipped
        assert len(result.warnings) > 0
        # Sanitized output should have empty results
        assert result.sanitized_output["results"] == []

    def test_enrichment_output_safety_column_blocked(self):
        """LLM must never return safety columns like allergens."""
        result = validate_enrichment_output(
            result={
                "results": [
                    {"idx": 0, "primary_category": "Dairy", "allergens": "milk, soy"}
                ]
            },
            batch_size=5,
            batch_indices=[0, 1, 2, 3, 4],
        )
        assert not result.passed
        assert any("safety column" in e for e in result.errors)
        # The safety column should be stripped from sanitized output
        sanitized_item = result.sanitized_output["results"][0]
        assert "allergens" not in sanitized_item

    def test_enrichment_output_is_organic_blocked(self):
        result = validate_enrichment_output(
            result={"results": [{"idx": 0, "primary_category": "X", "is_organic": True}]},
            batch_size=5,
            batch_indices=[0, 1, 2, 3, 4],
        )
        assert not result.passed
        assert any("is_organic" in e for e in result.errors)


# ============================================================================
# SECTION 7: Response-Level Guardrails
# ============================================================================


class TestResponseGuardrails:
    """Tests for response-level checks applied to raw LLM output."""

    def test_response_size_ok(self):
        assert check_response_size('{"ok": true}').passed

    def test_response_size_too_large(self):
        huge = "x" * 300_000
        result = check_response_size(huge)
        assert not result.passed
        assert "runaway" in result.errors[0]

    def test_json_parseable_valid(self):
        assert check_json_parseable('{"a": 1}').passed

    def test_json_parseable_markdown_fenced(self):
        result = check_json_parseable('```json\n{"a": 1}\n```')
        assert result.passed
        assert any("markdown" in w for w in result.warnings)

    def test_json_parseable_invalid(self):
        result = check_json_parseable("this is not json at all")
        assert not result.passed

    def test_no_prompt_leakage_clean(self):
        result = check_no_prompt_leakage('{"column_mapping": {}}')
        assert result.passed
        assert len(result.warnings) == 0

    def test_prompt_leakage_detected(self):
        response = '{"note": "You are a schema analysis agent for a data enrichment pipeline"}'
        result = check_no_prompt_leakage(response)
        assert result.passed  # leakage is a warning, not a block
        assert len(result.warnings) > 0


# ============================================================================
# SECTION 8: Composite Guardrail Runners
# ============================================================================


class TestCompositeGuardrails:
    """Tests for the top-level run_input_guardrails / run_output_guardrails."""

    def test_run_input_guardrails_unknown_site(self):
        result = run_input_guardrails("unknown_agent")
        assert result.passed  # unknown sites pass with a logged warning

    def test_run_input_guardrails_schema_analysis(self):
        result = run_input_guardrails(
            "schema_analysis",
            source_schema={"col": {}},
            unified_schema={"columns": {}},
        )
        assert result.passed

    def test_run_output_guardrails_blocks_on_size(self):
        result = run_output_guardrails(
            call_site="schema_analysis",
            raw_response="x" * 300_000,
            parsed_result={},
            source_columns=set(),
            unified_columns=set(),
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed

    def test_run_output_guardrails_valid_schema_analysis(self):
        raw = json.dumps({"column_mapping": {"a": "b"}, "operations": []})
        result = run_output_guardrails(
            call_site="schema_analysis",
            raw_response=raw,
            parsed_result=json.loads(raw),
            source_columns={"a"},
            unified_columns={"b"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert result.passed


# ============================================================================
# SECTION 9: Agent Integration Tests (mocked LLM calls)
# ============================================================================


try:
    import langgraph  # noqa: F401
    import pandas  # noqa: F401
    HAS_PIPELINE_DEPS = True
except ImportError:
    HAS_PIPELINE_DEPS = False

skip_no_pipeline_deps = pytest.mark.skipif(
    not HAS_PIPELINE_DEPS, reason="Pipeline dependencies (langgraph, pandas) not installed"
)


@skip_no_pipeline_deps
class TestAgentOrchestrator:
    """Integration tests for Agent 1 — orchestrator with mocked LLM."""

    def test_analyze_schema_node_parses_valid_response(self):
        from src.agents import orchestrator

        with patch.object(orchestrator, "call_llm_json") as mock_llm, \
             patch.object(orchestrator, "get_domain_schema") as mock_get_schema:

            mock_get_schema.return_value = UnifiedSchema(
                columns={
                    "product_name": ColumnSpec(type="string", required=True),
                    "brand_name": ColumnSpec(type="string"),
                }
            )
            mock_llm.return_value = {
                "column_mapping": {"description": "product_name"},
                "operations": [
                    {
                        "primitive": "ADD",
                        "target_column": "brand_name",
                        "target_type": "string",
                        "action": "set_null",
                    }
                ],
                "unresolvable": [],
            }

            state: PipelineState = {
                "source_schema": {
                    "description": {
                        "dtype": "object",
                        "null_rate": 0.0,
                        "sample_values": ["Milk", "Bread"],
                    },
                    "__meta__": {},
                },
                "domain": "nutrition",
            }

            result = orchestrator.analyze_schema_node(state)

            assert "column_mapping" in result
            assert result["column_mapping"] == {"description": "product_name"}
            assert "operations" in result

    def test_analyze_schema_node_skips_if_already_ran(self):
        from src.agents import orchestrator

        state: PipelineState = {"operations": [{"primitive": "ADD"}]}
        result = orchestrator.analyze_schema_node(state)
        assert result == {}


@skip_no_pipeline_deps
class TestAgentCritic:
    """Integration tests for Agent 2 — critic with mocked LLM."""

    def test_critique_node_returns_revised_ops(self):
        from src.agents import critic

        with patch.object(critic, "call_llm_json") as mock_llm, \
             patch.object(critic, "get_domain_schema") as mock_schema:

            mock_schema.return_value = UnifiedSchema(
                columns={"product_name": ColumnSpec(type="string", required=True)}
            )
            mock_llm.return_value = {
                "revised_operations": [
                    {"primitive": "ADD", "target_column": "x", "action": "set_default", "default_value": "USDA"}
                ],
                "critique_notes": [
                    {"rule": "Rule 2", "column": "x", "correction": "Changed to set_default"}
                ],
            }

            state: PipelineState = {
                "operations": [{"primitive": "ADD", "target_column": "x", "action": "set_null"}],
                "source_schema": {"col1": {"dtype": "string"}, "__meta__": {}},
                "column_mapping": {},
            }

            result = critic.critique_schema_node(state)

            assert result["revised_operations"][0]["action"] == "set_default"
            assert len(result["critique_notes"]) == 1

    def test_critique_node_skips_if_no_operations(self):
        from src.agents import critic

        state: PipelineState = {"operations": [], "source_schema": {}}
        result = critic.critique_schema_node(state)
        assert result == {}

    def test_critique_node_skips_if_already_ran(self):
        from src.agents import critic

        state: PipelineState = {"revised_operations": [{"primitive": "ADD"}]}
        result = critic.critique_schema_node(state)
        assert result == {}


@skip_no_pipeline_deps
class TestAgentSequencePlanner:
    """Integration tests for Agent 3 — sequence planner with mocked LLM."""

    def test_plan_sequence_node_returns_sequence(self):
        from src.agents import graph

        with patch.object(graph, "call_llm_json") as mock_llm, \
             patch.object(graph, "get_domain_schema") as mock_schema, \
             patch.object(graph, "BlockRegistry") as mock_registry_cls:

            mock_schema.return_value = UnifiedSchema(
                columns={"product_name": ColumnSpec(type="string", required=True)}
            )

            mock_reg = MagicMock()
            mock_reg.get_default_sequence.return_value = [
                "dq_score_pre", "strip_whitespace", "dq_score_post"
            ]
            mock_reg.get_blocks_with_metadata.return_value = [
                {"name": "dq_score_pre"}, {"name": "strip_whitespace"}, {"name": "dq_score_post"}
            ]
            mock_reg.is_stage.return_value = False
            mock_registry_cls.instance.return_value = mock_reg

            mock_llm.return_value = {
                "block_sequence": ["dq_score_pre", "strip_whitespace", "dq_score_post"],
                "reasoning": "Standard order",
            }

            state: PipelineState = {
                "source_schema": {"col1": {"dtype": "string"}},
                "domain": "nutrition",
                "gaps": [],
                "registry_misses": [],
                "block_registry_hits": {},
                "enable_enrichment": True,
            }

            result = graph.plan_sequence_node(state)

            assert "block_sequence" in result
            assert result["block_sequence"][0] == "dq_score_pre"
            assert result["block_sequence"][-1] == "dq_score_post"

    def test_plan_sequence_skips_if_already_set(self):
        from src.agents import graph

        state: PipelineState = {"block_sequence": ["dq_score_pre", "dq_score_post"]}
        result = graph.plan_sequence_node(state)
        assert result == {}


# ============================================================================
# SECTION 10: Guardrail Constants Validation
# ============================================================================


class TestGuardrailConstants:
    """Ensure guardrail constants match the pipeline's taxonomy."""

    def test_all_primitives_covered(self):
        expected = {"RENAME", "CAST", "FORMAT", "DELETE", "ADD", "SPLIT", "UNIFY", "DERIVE", "ENRICH_ALIAS"}
        assert VALID_PRIMITIVES == expected

    def test_safety_columns_match(self):
        expected = {"allergens", "is_organic", "dietary_tags"}
        assert SAFETY_COLUMNS == expected

    def test_guardrail_result_bool(self):
        assert bool(GuardrailResult(passed=True)) is True
        assert bool(GuardrailResult(passed=False, errors=["x"])) is False


# ============================================================================
# SECTION 11: HITL Threshold Guardrails
# ============================================================================


class TestHITLThresholds:
    """Tests for Human-in-the-Loop threshold-based flagging."""

    def test_no_flags_under_threshold(self):
        from src.agents.guardrails import check_hitl_thresholds

        result = check_hitl_thresholds(
            call_site="schema_analysis",
            parsed_result={
                "operations": [{"primitive": "ADD"}] * 5,
                "unresolvable": [],
            },
        )
        assert len(result) == 0

    def test_operation_count_triggers_hitl(self):
        from src.agents.guardrails import check_hitl_thresholds, HITL_OPERATION_COUNT_THRESHOLD

        ops = [{"primitive": "ADD"}] * (HITL_OPERATION_COUNT_THRESHOLD + 5)
        result = check_hitl_thresholds(
            call_site="schema_analysis",
            parsed_result={"operations": ops, "unresolvable": []},
        )
        assert len(result) >= 1
        assert result[0].triggered
        assert "operation" in result[0].reason.lower()
        assert result[0].actual_value == len(ops)

    def test_unresolvable_count_triggers_hitl(self):
        from src.agents.guardrails import check_hitl_thresholds, HITL_UNRESOLVABLE_THRESHOLD

        unresolvable = [{"target_column": f"col_{j}"} for j in range(HITL_UNRESOLVABLE_THRESHOLD + 1)]
        result = check_hitl_thresholds(
            call_site="schema_analysis",
            parsed_result={"operations": [], "unresolvable": unresolvable},
        )
        triggered = [f for f in result if f.threshold_name == "unresolvable_count"]
        assert len(triggered) == 1
        assert triggered[0].triggered

    def test_low_confidence_triggers_hitl(self):
        from src.agents.guardrails import check_hitl_thresholds, HITL_LOW_CONFIDENCE_THRESHOLD

        result = check_hitl_thresholds(
            call_site="schema_analysis",
            parsed_result={"operations": [{"primitive": "ADD"}], "unresolvable": []},
            confidence_scores=[0.2, 0.3, 0.4],  # avg = 0.3 < threshold
        )
        triggered = [f for f in result if f.threshold_name == "avg_confidence"]
        assert len(triggered) == 1
        assert triggered[0].actual_value == pytest.approx(0.3, abs=0.01)

    def test_high_confidence_no_flag(self):
        from src.agents.guardrails import check_hitl_thresholds

        result = check_hitl_thresholds(
            call_site="schema_analysis",
            parsed_result={"operations": [{"primitive": "ADD"}], "unresolvable": []},
            confidence_scores=[0.9, 0.85, 0.95],
        )
        triggered = [f for f in result if f.threshold_name == "avg_confidence"]
        assert len(triggered) == 0

    def test_enrichment_large_batch_triggers_hitl(self):
        from src.agents.guardrails import check_hitl_thresholds

        results = [{"idx": i, "primary_category": "Cat"} for i in range(60)]
        flags = check_hitl_thresholds(
            call_site="enrichment",
            parsed_result={"results": results},
        )
        assert len(flags) == 1
        assert flags[0].triggered
        assert "spot-check" in flags[0].reason

    def test_enrichment_small_batch_no_flag(self):
        from src.agents.guardrails import check_hitl_thresholds

        results = [{"idx": i, "primary_category": "Cat"} for i in range(10)]
        flags = check_hitl_thresholds(
            call_site="enrichment",
            parsed_result={"results": results},
        )
        assert len(flags) == 0


# ============================================================================
# SECTION 12: Numerical Clamping & Range Validation
# ============================================================================


class TestNumericalClamping:
    """Tests for value clamping and range validation utilities."""

    def test_clamp_within_range(self):
        from src.agents.guardrails import clamp_value
        assert clamp_value(0.5, 0.0, 1.0) == 0.5

    def test_clamp_below_min(self):
        from src.agents.guardrails import clamp_value
        assert clamp_value(-0.5, 0.0, 1.0) == 0.0

    def test_clamp_above_max(self):
        from src.agents.guardrails import clamp_value
        assert clamp_value(2.0, 0.0, 1.0) == 1.0

    def test_validate_confidence_normal(self):
        from src.agents.guardrails import validate_confidence_score
        assert validate_confidence_score(0.85) == 0.85

    def test_validate_confidence_clamps_high(self):
        from src.agents.guardrails import validate_confidence_score
        assert validate_confidence_score(1.5) == 1.0

    def test_validate_confidence_clamps_low(self):
        from src.agents.guardrails import validate_confidence_score
        assert validate_confidence_score(-0.2) == 0.0

    def test_validate_confidence_non_numeric(self):
        from src.agents.guardrails import validate_confidence_score
        assert validate_confidence_score("bad") == 0.0

    def test_validate_dq_score_normal(self):
        from src.agents.guardrails import validate_dq_score
        assert validate_dq_score(72.5) == 72.5

    def test_validate_dq_score_clamps(self):
        from src.agents.guardrails import validate_dq_score
        assert validate_dq_score(150.0) == 100.0
        assert validate_dq_score(-10.0) == 0.0

    def test_validate_risk_score_normal(self):
        from src.agents.guardrails import validate_risk_score
        assert validate_risk_score(3) == 3

    def test_validate_risk_score_clamps(self):
        from src.agents.guardrails import validate_risk_score
        assert validate_risk_score(0) == 1
        assert validate_risk_score(9) == 5

    def test_validate_risk_score_non_int(self):
        from src.agents.guardrails import validate_risk_score
        assert validate_risk_score("bad") == 3


# ============================================================================
# SECTION 13: Full Guardrail Pipeline with Audit
# ============================================================================


class TestGuardrailAudit:
    """Tests for the full guardrail pipeline with audit trail."""

    def test_audit_captures_metadata(self):
        from src.agents.guardrails import run_guardrails_with_audit, GuardrailAudit

        raw = json.dumps({"column_mapping": {"a": "b"}, "operations": [], "unresolvable": []})
        result, audit = run_guardrails_with_audit(
            call_site="schema_analysis",
            raw_response=raw,
            parsed_result=json.loads(raw),
            model_version="deepseek/deepseek-chat",
            source_columns={"a"},
            unified_columns={"b"},
            enrichment_columns=set(),
            computed_columns=set(),
        )

        assert result.passed
        assert isinstance(audit, GuardrailAudit)
        assert audit.call_site == "schema_analysis"
        assert audit.model_version == "deepseek/deepseek-chat"
        assert audit.elapsed_ms is not None
        assert audit.timestamp is not None
        assert not audit.requires_human_review

    def test_audit_flags_hitl_when_threshold_exceeded(self):
        from src.agents.guardrails import run_guardrails_with_audit

        ops = [{"primitive": "ADD", "target_column": f"c{i}", "action": "set_null"} for i in range(20)]
        parsed = {"column_mapping": {}, "operations": ops, "unresolvable": []}
        raw = json.dumps(parsed)

        result, audit = run_guardrails_with_audit(
            call_site="schema_analysis",
            raw_response=raw,
            parsed_result=parsed,
            model_version="deepseek/deepseek-chat",
            source_columns=set(),
            unified_columns={f"c{i}" for i in range(20)},
            enrichment_columns=set(),
            computed_columns=set(),
        )

        assert audit.requires_human_review
        assert any(f.threshold_name == "operation_count" for f in audit.hitl_flags)

    def test_audit_no_hitl_for_normal_output(self):
        from src.agents.guardrails import run_guardrails_with_audit

        parsed = {
            "column_mapping": {"a": "b"},
            "operations": [
                {"primitive": "ADD", "target_column": "x", "action": "set_null"},
            ],
            "unresolvable": [],
        }
        raw = json.dumps(parsed)
        result, audit = run_guardrails_with_audit(
            call_site="schema_analysis",
            raw_response=raw,
            parsed_result=parsed,
            model_version="test",
            source_columns={"a"},
            unified_columns={"b", "x"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert result.passed
        assert not audit.requires_human_review

    def test_audit_with_confidence_scores(self):
        from src.agents.guardrails import run_guardrails_with_audit

        parsed = {"column_mapping": {}, "operations": [{"primitive": "ADD", "target_column": "x", "action": "set_null"}], "unresolvable": []}
        raw = json.dumps(parsed)

        result, audit = run_guardrails_with_audit(
            call_site="schema_analysis",
            raw_response=raw,
            parsed_result=parsed,
            model_version="deepseek/deepseek-chat",
            confidence_scores=[0.3, 0.2, 0.4],
            source_columns=set(),
            unified_columns={"x"},
            enrichment_columns=set(),
            computed_columns=set(),
        )

        assert audit.requires_human_review
        assert "confidence_threshold" in audit.checks_performed


# ============================================================================
# SECTION 14: Hallucination Detection Tests
# ============================================================================


class TestHallucinationDetection:
    """Tests for hallucination-specific guardrails across all agents."""

    # --- Agent 1: Fabricated source columns ---

    def test_hallucinated_source_column_in_mapping_blocked(self):
        """LLM invents a column name that doesn't exist in the source data."""
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {"fake_column": "product_name"},
                "operations": [],
            },
            source_columns={"description", "brand"},
            unified_columns={"product_name"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed
        assert any("hallucinated source column" in e for e in result.errors)

    def test_hallucinated_source_column_in_operations_blocked(self):
        """LLM references a nonexistent source column in a CAST/FORMAT/DERIVE op."""
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {
                        "primitive": "FORMAT",
                        "source_column": "nonexistent_col",
                        "target_column": "published_date",
                        "action": "parse_date",
                    }
                ],
            },
            source_columns={"date_col", "name_col"},
            unified_columns={"published_date"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert not result.passed
        assert any("hallucinated source column" in e for e in result.errors)

    def test_valid_source_column_passes(self):
        """Real source column should not trigger hallucination error."""
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {
                        "primitive": "FORMAT",
                        "source_column": "date_col",
                        "target_column": "published_date",
                        "action": "parse_date",
                    }
                ],
            },
            source_columns={"date_col", "name_col"},
            unified_columns={"published_date"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert result.passed

    def test_add_primitive_no_source_column_ok(self):
        """ADD operations don't need a source column — should not trigger error."""
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {"primitive": "ADD", "target_column": "brand_name", "action": "set_null"}
                ],
            },
            source_columns={"desc"},
            unified_columns={"brand_name"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        assert result.passed

    # --- Agent 1: Duplicate operations (loop hallucination) ---

    def test_duplicate_operations_warned(self):
        """Same (primitive, target) repeated — hallucination loop signal."""
        result = validate_schema_analysis_output(
            result={
                "column_mapping": {},
                "operations": [
                    {"primitive": "ADD", "target_column": "x", "action": "set_null"},
                    {"primitive": "ADD", "target_column": "x", "action": "set_null"},
                ],
            },
            source_columns=set(),
            unified_columns={"x"},
            enrichment_columns=set(),
            computed_columns=set(),
        )
        # Duplicates are warnings not errors (the first one is valid)
        assert any("duplicate" in w.lower() for w in result.warnings)

    # --- Agent 2: Hallucinated target columns ---

    def test_critic_hallucinated_target_column_warned(self):
        """Critic invents a target column not in the unified schema."""
        result = validate_critic_output(
            result={
                "revised_operations": [
                    {"primitive": "ADD", "target_column": "invented_col", "action": "set_null"}
                ],
                "critique_notes": [],
            },
            original_operations=[{"primitive": "ADD", "target_column": "x"}],
            unified_columns={"product_name", "brand_name", "x"},
        )
        assert result.passed  # warning, not error — critic might have valid reason
        assert any("hallucinated" in w or "not in unified schema" in w for w in result.warnings)

    def test_critic_valid_target_no_warning(self):
        """Critic targeting a real unified column should not warn."""
        result = validate_critic_output(
            result={
                "revised_operations": [
                    {"primitive": "ADD", "target_column": "brand_name", "action": "set_default", "default_value": "USDA"}
                ],
                "critique_notes": [],
            },
            original_operations=[{"primitive": "ADD", "target_column": "brand_name"}],
            unified_columns={"product_name", "brand_name"},
        )
        assert result.passed
        assert not any("not in unified schema" in w for w in result.warnings)

    # --- Agent 3: Fabricated blocks ---

    def test_sequence_planner_hallucinated_blocks(self):
        """Planner invents blocks that don't exist in the registry."""
        result = validate_sequence_planner_output(
            result={
                "block_sequence": [
                    "dq_score_pre", "ai_magic_transform", "neural_cleaner", "dq_score_post"
                ]
            },
            required_blocks=["dq_score_pre", "strip_whitespace", "dq_score_post"],
        )
        assert not result.passed
        assert any("hallucinated" in e.lower() for e in result.errors)

    # --- S3 Enrichment: Hallucinated categories ---

    def test_enrichment_hallucinated_category_warned(self):
        """LLM returns a category not in the allowed list."""
        from src.agents.guardrails import validate_enrichment_output

        result = validate_enrichment_output(
            result={
                "results": [
                    {"idx": 0, "primary_category": "Quantum Electronics"},
                ]
            },
            batch_size=5,
            batch_indices=[0, 1, 2, 3, 4],
        )
        # Hallucinated categories are warned but still kept (might be edge case)
        assert any("not in the allowed categories" in w for w in result.warnings)

    def test_enrichment_valid_category_no_warning(self):
        from src.agents.guardrails import validate_enrichment_output

        result = validate_enrichment_output(
            result={
                "results": [
                    {"idx": 0, "primary_category": "Dairy"},
                ]
            },
            batch_size=5,
            batch_indices=[0, 1, 2, 3, 4],
        )
        assert result.passed
        assert not any("categories" in w for w in result.warnings)

    def test_enrichment_null_category_allowed(self):
        """LLM returning null means 'unsure' — this is preferred over hallucination."""
        from src.agents.guardrails import validate_enrichment_output

        result = validate_enrichment_output(
            result={
                "results": [
                    {"idx": 0, "primary_category": None},
                ]
            },
            batch_size=5,
            batch_indices=[0, 1, 2, 3, 4],
        )
        assert result.passed
        assert not any("categories" in w for w in result.warnings)
