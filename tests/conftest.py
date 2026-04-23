"""Shared pytest fixtures for the Marketplace Intelligence Platform test suite."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_source_schema() -> dict:
    return {
        "fdc_id": {
            "dtype": "int64",
            "null_rate": 0.0,
            "unique_count": 100,
            "detected_structure": "scalar",
        },
        "description": {
            "dtype": "object",
            "null_rate": 0.05,
            "unique_count": 95,
            "detected_structure": "scalar",
        },
        "brand_owner": {
            "dtype": "object",
            "null_rate": 0.20,
            "unique_count": 50,
            "detected_structure": "scalar",
        },
        "ingredients": {
            "dtype": "object",
            "null_rate": 0.10,
            "unique_count": 90,
            "detected_structure": "scalar",
        },
        "__meta__": {
            "row_count": 100,
            "sampling_strategy": {"method": "random", "sample_size": 100},
        },
    }


@pytest.fixture
def sample_unified_schema() -> dict:
    return {
        "columns": {
            "product_id": {"type": "string", "required": True},
            "product_name": {"type": "string", "required": True},
            "brand_name": {"type": "string", "required": False},
            "ingredients": {"type": "string", "required": False},
            "primary_category": {"type": "string", "enrichment": True},
            "allergens": {"type": "string", "enrichment": True},
            "is_organic": {"type": "boolean", "enrichment": True},
            "dietary_tags": {"type": "string", "enrichment": True},
            "dq_score_pre": {"type": "float", "computed": True},
            "dq_score_post": {"type": "float", "computed": True},
            "dq_delta": {"type": "float", "computed": True},
        }
    }


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_name": ["Cheerios", "Cornflakes", "Granola Bar"],
            "brand_name": ["General Mills", "Kelloggs", "Nature Valley"],
            "ingredients": [
                "whole grain oats, sugar, salt",
                "milled corn, sugar, malt flavor",
                "whole grain oats, sugar, canola oil",
            ],
            "published_date": pd.to_datetime(
                ["2025-01-15", "2024-06-20", "2025-09-10"]
            ),
        }
    )
