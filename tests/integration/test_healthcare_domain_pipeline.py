"""Integration test: run pipeline with healthcare_test domain (T021).

Validates SC-002: zero food-domain columns in output.
Marks @pytest.mark.integration — excluded from default pytest run with -m "not integration".
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

FIXTURE_CSV = Path(__file__).resolve().parent.parent / "fixtures" / "healthcare_sample.csv"
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_FILE = PROJECT_ROOT / "config" / "schemas" / "healthcare_test_schema.json"
OUTPUT_DIR = PROJECT_ROOT / "output"

FOOD_COLUMNS = {"allergens", "dietary_tags", "is_organic", "primary_category"}


@pytest.mark.integration
def test_healthcare_pipeline_no_food_columns():
    """Full pipeline run on healthcare CSV. Zero food columns in output."""
    pytest.importorskip("src.pipeline.cli")

    assert FIXTURE_CSV.exists(), f"Test fixture missing: {FIXTURE_CSV}"

    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable, "-m", "src.pipeline.cli",
            "--source", str(FIXTURE_CSV),
            "--domain", "healthcare_test",
            "--force-fresh",
        ],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=300,
    )

    assert result.returncode == 0, (
        f"Pipeline failed with exit code {result.returncode}\n"
        f"STDOUT:\n{result.stdout[-3000:]}\n"
        f"STDERR:\n{result.stderr[-3000:]}"
    )

    # Find the output CSV
    output_files = list(OUTPUT_DIR.glob("healthcare_sample*.csv")) + list(OUTPUT_DIR.glob("*healthcare*.csv"))
    assert output_files, f"No output CSV found in {OUTPUT_DIR}"

    out_df = pd.read_csv(output_files[0])

    # SC-002: zero food columns
    food_cols_in_output = FOOD_COLUMNS & set(out_df.columns)
    assert not food_cols_in_output, (
        f"Food-domain columns found in healthcare output: {food_cols_in_output}. "
        "Tier 2 parameterization not working."
    )

    # Healthcare-specific columns should be present
    assert "icd10_codes" in out_df.columns, "icd10_codes column missing from output"

    # Rows with ICD patterns should have non-null icd10_codes
    rows_with_icd = out_df[out_df["icd10_codes"].notna() & (out_df["icd10_codes"] != "")]
    assert len(rows_with_icd) > 0, "No rows had ICD codes extracted"

    # DQ columns must be present
    assert "dq_score_pre" in out_df.columns
    assert "dq_score_post" in out_df.columns


@pytest.fixture(autouse=True)
def cleanup_schema():
    """Remove healthcare_test schema after test so it doesn't pollute other tests."""
    yield
    if SCHEMA_FILE.exists():
        SCHEMA_FILE.unlink()
