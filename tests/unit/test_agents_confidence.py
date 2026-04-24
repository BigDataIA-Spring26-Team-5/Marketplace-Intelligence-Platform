"""Unit tests for src.agents.confidence."""

from __future__ import annotations

import pytest

from src.agents.confidence import (
    ConfidenceScore,
    calculate_confidence,
    get_confidence_level,
    get_confidence_display,
)


class TestConfidenceScoreDataclass:
    def test_construction_defaults(self):
        s = ConfidenceScore(score=0.5, factors=["a"])
        assert s.score == 0.5
        assert s.factors == ["a"]
        assert s.evidence_sample is None

    def test_construction_with_evidence(self):
        s = ConfidenceScore(score=0.9, factors=["x"], evidence_sample=["v1", "v2"])
        assert s.evidence_sample == ["v1", "v2"]


class TestCalculateConfidenceNullRate:
    def test_low_null_rate_factor(self):
        r = calculate_confidence(null_rate=0.05, unique_count=10, sample_size=1000)
        assert "low_null_rate" in r.factors

    def test_medium_null_rate_factor(self):
        r = calculate_confidence(null_rate=0.2, unique_count=10, sample_size=1000)
        assert "medium_null_rate" in r.factors

    def test_high_null_rate_factor(self):
        r = calculate_confidence(null_rate=0.4, unique_count=10, sample_size=1000)
        assert "high_null_rate" in r.factors

    def test_very_high_null_rate_factor(self):
        r = calculate_confidence(null_rate=0.8, unique_count=10, sample_size=1000)
        assert "very_high_null_rate" in r.factors


class TestCalculateConfidenceSourceColumn:
    def test_has_source_factor(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, has_source_column=True)
        assert "source_column_exists" in r.factors

    def test_no_source_factor(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, has_source_column=False)
        assert "no_source_column" in r.factors
        # no_source should dampen score vs has_source
        with_src = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, has_source_column=True).score
        no_src = r.score
        assert no_src < with_src


class TestCalculateConfidenceSampleSize:
    def test_adequate_sample(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000)
        assert "adequate_sample_size" in r.factors

    def test_moderate_sample(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=200)
        assert "moderate_sample_size" in r.factors

    def test_small_sample(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=50)
        assert "small_sample_size" in r.factors


class TestCalculateConfidenceTypeConsistency:
    def test_high_consistency(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, type_consistency=0.95)
        assert "high_type_consistency" in r.factors

    def test_medium_consistency(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, type_consistency=0.75)
        assert "medium_type_consistency" in r.factors

    def test_low_consistency(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, type_consistency=0.5)
        assert "low_type_consistency" in r.factors


class TestCalculateConfidenceStructure:
    def test_scalar(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="scalar")
        assert "scalar_structure" in r.factors

    def test_json_array(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="json_array")
        assert "json_structure" in r.factors

    def test_json_object(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="json_object")
        assert "json_structure" in r.factors

    def test_delimited(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="delimited")
        assert "complex_structure" in r.factors

    def test_composite(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="composite")
        assert "complex_structure" in r.factors

    def test_unknown(self):
        r = calculate_confidence(null_rate=0.0, unique_count=10, sample_size=1000, detected_structure="weird")
        assert "unknown_structure" in r.factors


class TestCalculateConfidenceScoreBounds:
    def test_score_clamped_0_1(self):
        r = calculate_confidence(null_rate=0.0, unique_count=1000, sample_size=10000)
        assert 0.0 <= r.score <= 1.0

    def test_perfect_inputs_high_score(self):
        r = calculate_confidence(
            null_rate=0.0, unique_count=100, sample_size=10000,
            has_source_column=True, type_consistency=1.0, detected_structure="scalar",
        )
        assert r.score == 1.0

    def test_worst_inputs_low_score(self):
        r = calculate_confidence(
            null_rate=1.0, unique_count=0, sample_size=1,
            has_source_column=False, type_consistency=0.0, detected_structure="unknown",
        )
        assert r.score == 0.0


class TestGetConfidenceLevel:
    def test_high(self):
        assert get_confidence_level(0.95) == "high"
        assert get_confidence_level(0.9) == "high"

    def test_medium(self):
        assert get_confidence_level(0.75) == "medium"
        assert get_confidence_level(0.5) == "medium"

    def test_low(self):
        assert get_confidence_level(0.3) == "low"
        assert get_confidence_level(0.0) == "low"


class TestGetConfidenceDisplay:
    def test_high(self):
        icon, text = get_confidence_display(0.95)
        assert "High" in text

    def test_medium(self):
        icon, text = get_confidence_display(0.7)
        assert "Medium" in text

    def test_low(self):
        icon, text = get_confidence_display(0.2)
        assert "Low" in text
