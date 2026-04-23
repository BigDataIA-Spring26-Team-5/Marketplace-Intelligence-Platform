"""Tests for src/schema/models.py — Pydantic UnifiedSchema, ColumnSpec, DQWeights."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from src.schema.models import ColumnSpec, DQWeights, UnifiedSchema


# ---------------------------------------------------------------------------
# ColumnSpec
# ---------------------------------------------------------------------------


class TestColumnSpec:
    def test_minimal_spec(self):
        spec = ColumnSpec(type="string")
        assert spec.type == "string"
        assert spec.required is False
        assert spec.enrichment is False
        assert spec.computed is False
        assert spec.enrichment_alias is None

    def test_all_valid_types(self):
        for t in ("string", "float", "integer", "boolean"):
            spec = ColumnSpec(type=t)
            assert spec.type == t

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            ColumnSpec(type="datetime")  # type: ignore[arg-type]

    def test_extra_fields_allowed(self):
        # extra=allow ensures forward compatibility with new flags
        spec = ColumnSpec(type="string", future_flag="x")  # type: ignore[call-arg]
        assert spec.type == "string"

    def test_enrichment_alias_field(self):
        spec = ColumnSpec(type="string", enrichment_alias="external_category")
        assert spec.enrichment_alias == "external_category"


# ---------------------------------------------------------------------------
# DQWeights
# ---------------------------------------------------------------------------


class TestDQWeights:
    def test_default_weights_sum_to_one(self):
        w = DQWeights()
        total = w.completeness + w.freshness + w.ingredient_richness
        assert abs(total - 1.0) < 1e-9

    def test_custom_weights_summing_to_one(self):
        w = DQWeights(completeness=0.5, freshness=0.3, ingredient_richness=0.2)
        assert w.completeness == 0.5

    def test_weights_must_sum_to_one(self):
        with pytest.raises(ValidationError) as exc:
            DQWeights(completeness=0.5, freshness=0.3, ingredient_richness=0.3)
        assert "sum to 1.0" in str(exc.value)

    def test_floating_point_tolerance(self):
        # 1e-6 tolerance per the validator
        DQWeights(
            completeness=0.4 + 1e-7, freshness=0.35, ingredient_richness=0.25
        )

    def test_zero_weights_fail(self):
        with pytest.raises(ValidationError):
            DQWeights(completeness=0.0, freshness=0.0, ingredient_richness=0.0)


# ---------------------------------------------------------------------------
# UnifiedSchema
# ---------------------------------------------------------------------------


class TestUnifiedSchema:
    @pytest.fixture
    def sample_schema(self) -> UnifiedSchema:
        return UnifiedSchema(
            columns={
                "product_id": ColumnSpec(type="string", required=True),
                "product_name": ColumnSpec(type="string", required=True),
                "brand_name": ColumnSpec(type="string", required=False),
                "primary_category": ColumnSpec(type="string", enrichment=True),
                "allergens": ColumnSpec(type="string", enrichment=True),
                "dq_score_pre": ColumnSpec(type="float", computed=True),
                "dq_score_post": ColumnSpec(type="float", computed=True),
                "dq_delta": ColumnSpec(type="float", computed=True),
            },
        )

    def test_required_columns_excludes_computed(self, sample_schema):
        required = sample_schema.required_columns
        assert "product_id" in required
        assert "product_name" in required
        assert "dq_score_pre" not in required

    def test_required_columns_excludes_unrequired(self, sample_schema):
        assert "brand_name" not in sample_schema.required_columns

    def test_mappable_columns_excludes_computed_and_enrichment(self, sample_schema):
        mappable = sample_schema.mappable_columns
        assert "product_id" in mappable
        assert "brand_name" in mappable
        assert "primary_category" not in mappable  # enrichment
        assert "allergens" not in mappable  # enrichment
        assert "dq_score_pre" not in mappable  # computed

    def test_enrichment_columns(self, sample_schema):
        enrich = sample_schema.enrichment_columns
        assert "primary_category" in enrich
        assert "allergens" in enrich
        assert "product_name" not in enrich

    def test_for_prompt_excludes_computed(self, sample_schema):
        prompt_dict = sample_schema.for_prompt()
        assert "dq_score_pre" not in prompt_dict["columns"]
        assert "dq_score_post" not in prompt_dict["columns"]
        # Enrichment columns ARE included in the prompt — Agent 1 needs to know
        # about them so it can emit ENRICH_ALIAS ops, just not map source data to them.
        assert "primary_category" in prompt_dict["columns"]
        assert "product_name" in prompt_dict["columns"]

    def test_for_prompt_is_json_serializable(self, sample_schema):
        # for_prompt() must return a plain dict usable with json.dumps()
        json.dumps(sample_schema.for_prompt())

    def test_to_json_roundtrip(self, sample_schema):
        as_json = sample_schema.to_json()
        loaded = json.loads(as_json)
        rebuilt = UnifiedSchema.model_validate(loaded)
        assert rebuilt.required_columns == sample_schema.required_columns

    def test_default_dq_weights_attached(self):
        schema = UnifiedSchema(columns={"x": ColumnSpec(type="string")})
        assert isinstance(schema.dq_weights, DQWeights)
        assert abs(
            schema.dq_weights.completeness
            + schema.dq_weights.freshness
            + schema.dq_weights.ingredient_richness
            - 1.0
        ) < 1e-9

    def test_custom_dq_weights_validated(self):
        with pytest.raises(ValidationError):
            UnifiedSchema(
                columns={"x": ColumnSpec(type="string")},
                dq_weights=DQWeights(
                    completeness=0.9, freshness=0.05, ingredient_richness=0.5
                ),
            )

    def test_empty_columns_allowed(self):
        # No validation on minimum number of columns at the model level
        schema = UnifiedSchema(columns={})
        assert schema.mappable_columns == {}
        assert schema.required_columns == set()
