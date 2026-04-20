"""Confidence scoring for gap classifications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ConfidenceScore:
    """Confidence score for a gap classification."""

    score: float  # 0.0 to 1.0
    factors: list[str]  # List of factors that contributed to the score
    evidence_sample: Optional[list[str]] = (
        None  # Sample values that support this classification
    )


def calculate_confidence(
    null_rate: float,
    unique_count: int,
    sample_size: int,
    has_source_column: bool = True,
    type_consistency: float = 1.0,
    detected_structure: str = "scalar",
) -> ConfidenceScore:
    """
    Calculate confidence score for a gap classification based on data characteristics.

    The confidence formula considers:
    - null_rate: Lower null rate = higher confidence
    - unique_count: Higher unique count = more representative sample
    - sample_size: Larger sample = more confident
    - has_source_column: Whether a source column was identified
    - type_consistency: Consistency of data types in the column
    - detected_structure: Structural pattern detected

    Args:
        null_rate: Fraction of null values (0.0-1.0)
        unique_count: Number of unique non-null values
        sample_size: Number of rows in the sample
        has_source_column: Whether source column exists for mapping
        type_consistency: Type consistency score (0.0-1.0)
        detected_structure: Structural pattern detected

    Returns:
        ConfidenceScore with score (0.0-1.0) and contributing factors
    """
    factors = []

    # Factor 1: Null rate (most important)
    # Low null rate = high confidence
    if null_rate < 0.1:
        factors.append("low_null_rate")
        null_factor = 1.0
    elif null_rate < 0.3:
        factors.append("medium_null_rate")
        null_factor = 0.8
    elif null_rate < 0.5:
        factors.append("high_null_rate")
        null_factor = 0.5
    else:
        factors.append("very_high_null_rate")
        null_factor = 0.2

    # Factor 2: Source column presence
    if has_source_column:
        factors.append("source_column_exists")
        source_factor = 1.0
    else:
        factors.append("no_source_column")
        source_factor = 0.3

    # Factor 3: Sample size adequacy
    # Larger samples give more confidence
    if sample_size >= 500:
        factors.append("adequate_sample_size")
        sample_factor = 1.0
    elif sample_size >= 100:
        factors.append("moderate_sample_size")
        sample_factor = 0.7
    else:
        factors.append("small_sample_size")
        sample_factor = 0.4

    # Factor 4: Type consistency
    if type_consistency >= 0.9:
        factors.append("high_type_consistency")
        type_factor = 1.0
    elif type_consistency >= 0.7:
        factors.append("medium_type_consistency")
        type_factor = 0.7
    else:
        factors.append("low_type_consistency")
        type_factor = 0.4

    # Factor 5: Structural detection confidence
    if detected_structure == "scalar":
        structure_factor = 1.0
        factors.append("scalar_structure")
    elif detected_structure in ("json_array", "json_object"):
        structure_factor = 0.9
        factors.append("json_structure")
    elif detected_structure in ("delimited", "composite"):
        structure_factor = 0.8
        factors.append("complex_structure")
    else:
        structure_factor = 0.6
        factors.append("unknown_structure")

    # Calculate combined score
    # Weighted formula:
    # confidence = null_rate_factor * source_factor * sample_factor * type_factor * structure_factor
    # But we invert null_rate since lower is better
    confidence = (
        (1 - null_rate)  # Invert: lower null = higher confidence
        * source_factor
        * sample_factor
        * type_factor
        * structure_factor
    )

    # Clamp to 0.0-1.0
    confidence = max(0.0, min(1.0, confidence))

    return ConfidenceScore(score=confidence, factors=factors)


def get_confidence_level(score: float) -> str:
    """
    Categorize confidence score into level.

    Args:
        score: Confidence score (0.0-1.0)

    Returns:
        "high", "medium", or "low"
    """
    if score >= 0.9:
        return "high"
    elif score >= 0.5:
        return "medium"
    else:
        return "low"


def get_confidence_display(score: float) -> tuple[str, str]:
    """
    Get display text and icon for confidence score.

    Args:
        score: Confidence score (0.0-1.0)

    Returns:
        Tuple of (icon, level_text)
    """
    level = get_confidence_level(score)

    if level == "high":
        return "✅", "High (≥90%)"
    elif level == "medium":
        return "⚠️", "Medium (50-89%)"
    else:
        return "❌", "Low (<50%)"
