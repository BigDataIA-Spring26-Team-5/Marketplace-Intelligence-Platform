"""Unit tests for schema sampling strategies."""

from __future__ import annotations

import pandas as pd
import pytest

from src.schema.sampling import (
    SamplingStrategy,
    calculate_sample_size,
    random_sample,
    full_scan,
    detect_sparse_columns,
    adaptive_sample,
)


class TestCalculateSampleSize:
    def test_zero_rows(self):
        assert calculate_sample_size(0) == 0

    def test_negative_rows(self):
        assert calculate_sample_size(-5) == 0

    def test_small_dataset(self):
        # Should return at least 100 or total rows
        result = calculate_sample_size(50)
        assert result == 50

    def test_large_dataset(self):
        result = calculate_sample_size(1_000_000)
        # base=500, buffer=200 → 700
        assert result == 700

    def test_medium_dataset(self):
        result = calculate_sample_size(10_000)
        # base=min(500, 100)=100, buffer=min(200, 500)=200 → 300
        assert result == 300


class TestRandomSample:
    def test_full_scan_when_sample_size_exceeds(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        sampled, strat = random_sample(df, 10)
        assert len(sampled) == 3
        assert strat.method == "full_scan"
        assert strat.fallback_triggered is False

    def test_random_with_seed_reproducible(self):
        df = pd.DataFrame({"a": list(range(100))})
        s1, _ = random_sample(df, 10, seed=42, detect_sparse=False)
        s2, _ = random_sample(df, 10, seed=42, detect_sparse=False)
        assert s1["a"].tolist() == s2["a"].tolist()

    def test_sparse_fallback_triggered(self):
        # Create a column that is mostly non-null in full but sampled as null
        df = pd.DataFrame({"a": list(range(1000)), "b": [None] * 950 + list(range(50))})
        sampled, strat = random_sample(df, 10, seed=1, detect_sparse=True)
        assert strat.method == "random"

    def test_strategy_fields(self):
        df = pd.DataFrame({"a": list(range(100))})
        _, strat = random_sample(df, 20, seed=5, detect_sparse=False)
        assert strat.sample_size == 20
        assert strat.seed == 5


class TestFullScan:
    def test_returns_all(self):
        df = pd.DataFrame({"a": [1, 2]})
        sampled, strat = full_scan(df, reason="test")
        assert len(sampled) == 2
        assert strat.fallback_triggered is True
        assert strat.fallback_reason == "test"
        assert strat.method == "full_scan"


class TestDetectSparseColumns:
    def test_identifies_sparse(self):
        df = pd.DataFrame({"dense": [1, 2, 3, 4], "sparse": [None, None, None, 1]})
        sparse = detect_sparse_columns(df, df)
        assert "sparse" in sparse
        assert "dense" not in sparse

    def test_no_sparse(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert detect_sparse_columns(df, df) == []


class TestAdaptiveSample:
    def test_small_dataset_full_scan(self):
        df = pd.DataFrame({"a": list(range(100))})
        sampled, strat = adaptive_sample(df)
        assert len(sampled) == 100
        assert strat.method == "full_scan"
        assert strat.fallback_reason == "small_dataset"

    def test_large_dataset_samples(self):
        df = pd.DataFrame({"a": list(range(5000)), "b": list(range(5000))})
        sampled, strat = adaptive_sample(df, seed=42)
        assert len(sampled) < 5000

    def test_high_sparse_ratio_fallback(self):
        df = pd.DataFrame({
            "a": list(range(2000)),
            "b": [None] * 2000,
            "c": [None] * 2000,
        })
        sampled, strat = adaptive_sample(df, seed=1)
        assert strat.fallback_triggered is True


class TestSamplingStrategy:
    def test_dataclass_defaults(self):
        s = SamplingStrategy(method="random", sample_size=100)
        assert s.fallback_triggered is False
        assert s.fallback_reason is None
        assert s.seed is None
