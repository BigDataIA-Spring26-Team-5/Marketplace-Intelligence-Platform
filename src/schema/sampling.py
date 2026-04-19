"""Sampling utilities for representative row selection in schema analysis."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class SamplingStrategy:
    """Defines the sampling approach for a dataset."""

    method: str  # "full_scan", "random", "stratified"
    sample_size: int
    fallback_triggered: bool = False
    fallback_reason: Optional[str] = None
    seed: Optional[int] = None


def calculate_sample_size(total_rows: int, min_sample: int = 500) -> int:
    """
    Calculate the appropriate sample size based on dataset characteristics.

    Formula: min(500, sqrt(n)) + buffer for sparse column detection

    Args:
        total_rows: Total number of rows in the dataset
        min_sample: Minimum sample size (default: 500)

    Returns:
        Recommended sample size
    """
    if total_rows <= 0:
        return 0

    # Base sample: sqrt(n) capped at 500
    base = min(min_sample, int(math.sqrt(total_rows)))

    # Buffer for sparse column detection (up to 200 extra or 5% of total)
    buffer = min(200, total_rows // 20)

    sample_size = min(base + buffer, total_rows)

    return max(sample_size, min(100, total_rows))  # Ensure minimum 100 or full dataset


def random_sample(
    df: pd.DataFrame,
    sample_size: int,
    seed: Optional[int] = None,
    detect_sparse: bool = True,
) -> tuple[pd.DataFrame, SamplingStrategy]:
    """
    Take a random sample from the DataFrame with optional sparse column detection.

    Args:
        df: Input DataFrame
        sample_size: Number of rows to sample
        seed: Random seed for reproducibility
        detect_sparse: Whether to check for sparse columns and trigger fallback

    Returns:
        Tuple of (sampled DataFrame, SamplingStrategy)
    """
    if sample_size >= len(df):
        # Full scan for small datasets
        return df.copy(), SamplingStrategy(
            method="full_scan", sample_size=len(df), fallback_triggered=False
        )

    if seed is not None:
        random.seed(seed)

    # Random sampling
    sampled_indices = random.sample(range(len(df)), sample_size)
    sampled_df = df.iloc[sampled_indices].copy()

    # Check for sparse columns and determine if fallback needed
    fallback_triggered = False
    fallback_reason = None

    if detect_sparse:
        sparse_cols = detect_sparse_columns(df, sampled_df)
        if len(sparse_cols) > 0:
            # Check if any column has >80% null rate in sample but non-null in full
            for col in sparse_cols:
                sample_null_rate = sampled_df[col].isna().mean()
                full_null_rate = df[col].isna().mean()

                # If sample shows 80%+ null but full dataset has significantly fewer nulls,
                # we may have missed data - trigger warning
                if sample_null_rate > 0.8 and full_null_rate < sample_null_rate - 0.1:
                    fallback_triggered = True
                    fallback_reason = f"sparse_column_detected:{col}"

    strategy = SamplingStrategy(
        method="random",
        sample_size=sample_size,
        fallback_triggered=fallback_triggered,
        fallback_reason=fallback_reason,
        seed=seed,
    )

    return sampled_df, strategy


def full_scan(
    df: pd.DataFrame, reason: str = "high_null_rate"
) -> tuple[pd.DataFrame, SamplingStrategy]:
    """
    Return full DataFrame as sample when fallback is needed.

    Args:
        df: Input DataFrame
        reason: Reason for fallback

    Returns:
        Tuple of (full DataFrame, SamplingStrategy with fallback flag)
    """
    return df.copy(), SamplingStrategy(
        method="full_scan",
        sample_size=len(df),
        fallback_triggered=True,
        fallback_reason=reason,
    )


def detect_sparse_columns(df: pd.DataFrame, sample_df: pd.DataFrame) -> list[str]:
    """
    Identify columns that may be sparse (high null rate in sample).

    Args:
        df: Full DataFrame
        sample_df: Sampled DataFrame

    Returns:
        List of column names that appear sparse
    """
    sparse_columns = []

    for col in df.columns:
        sample_null_rate = sample_df[col].isna().mean()

        # Consider sparse if >50% null in sample
        if sample_null_rate > 0.5:
            sparse_columns.append(col)

    return sparse_columns


def adaptive_sample(
    df: pd.DataFrame, seed: Optional[int] = None, sparse_threshold: float = 0.8
) -> tuple[pd.DataFrame, SamplingStrategy]:
    """
    Adaptively choose sampling strategy based on dataset size and characteristics.

    This is the main entry point for sampling in the pipeline.

    Args:
        df: Input DataFrame
        seed: Random seed for reproducibility
        sparse_threshold: Null rate threshold for triggering full scan fallback

    Returns:
        Tuple of (sampled DataFrame, SamplingStrategy)
    """
    total_rows = len(df)

    # For small datasets, use full scan
    if total_rows <= 500:
        return full_scan(df, reason="small_dataset")

    # Calculate sample size based on formula
    sample_size = calculate_sample_size(total_rows)

    # Take initial sample
    sampled_df, strategy = random_sample(df, sample_size, seed, detect_sparse=True)

    # Check if fallback to full scan is needed
    # If more than 50% of columns are sparse, consider full scan
    sparse_cols = detect_sparse_columns(df, sampled_df)
    sparse_ratio = len(sparse_cols) / len(df.columns) if len(df.columns) > 0 else 0

    if sparse_ratio > 0.5:
        # High proportion of sparse columns - fall back to full scan
        return full_scan(df, reason=f"high_sparse_ratio:{sparse_ratio:.2f}")

    # Check for individual columns exceeding threshold
    for col in sparse_cols:
        sample_null_rate = sampled_df[col].isna().mean()
        if sample_null_rate > sparse_threshold:
            # This column has >80% null in sample - fallback to full scan
            return full_scan(df, reason=f"column_sparse_threshold:{col}")

    return sampled_df, strategy
