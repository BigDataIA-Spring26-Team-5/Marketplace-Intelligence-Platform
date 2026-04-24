"""Standalone smoke-test script for the healthcare_test domain pack.

Run from repo root:
    poetry run python scripts/test_healthcare_domain.py

No pytest required. Exits 0 on success, 1 on any failure.
Cleans up generated schema file after run.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_CSV = PROJECT_ROOT / "tests" / "fixtures" / "healthcare_sample.csv"
SCHEMA_FILE = PROJECT_ROOT / "config" / "schemas" / "healthcare_test_schema.json"
OUTPUT_DIR = PROJECT_ROOT / "output"

FOOD_COLUMNS = {"allergens", "dietary_tags", "is_organic", "primary_category"}

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def _check(label: str, condition: bool, detail: str = "") -> bool:
    status = PASS if condition else FAIL
    suffix = f"  — {detail}" if detail else ""
    print(f"  [{status}] {label}{suffix}")
    return condition


def run_pipeline() -> subprocess.CompletedProcess:
    print("\n=== Running healthcare_test pipeline ===")
    cmd = [
        sys.executable, "-m", "src.pipeline.cli",
        "--source", str(FIXTURE_CSV),
        "--domain", "healthcare_test",
        "--force-fresh",
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=300)


def validate_output(result: subprocess.CompletedProcess) -> list[bool]:
    checks: list[bool] = []

    checks.append(_check(
        "Pipeline exit code 0",
        result.returncode == 0,
        f"exit={result.returncode}" if result.returncode != 0 else "",
    ))

    if result.returncode != 0:
        print("\n  --- STDOUT (last 2000 chars) ---")
        print(result.stdout[-2000:])
        print("\n  --- STDERR (last 2000 chars) ---")
        print(result.stderr[-2000:])
        return checks

    output_files = (
        list(OUTPUT_DIR.glob("healthcare_sample*.csv"))
        + list(OUTPUT_DIR.glob("*healthcare*.csv"))
    )
    checks.append(_check("Output CSV exists", bool(output_files)))
    if not output_files:
        return checks

    df = pd.read_csv(output_files[0])
    print(f"\n  Output: {output_files[0].name}  ({len(df)} rows × {len(df.columns)} cols)")

    # SC-002: no food columns
    food_in_output = FOOD_COLUMNS & set(df.columns)
    checks.append(_check(
        "SC-002: zero food-domain columns",
        not food_in_output,
        f"found: {food_in_output}" if food_in_output else "",
    ))

    # ICD-10 extraction
    checks.append(_check("icd10_codes column present", "icd10_codes" in df.columns))
    if "icd10_codes" in df.columns:
        non_null = df["icd10_codes"].notna().sum()
        checks.append(_check(
            f"icd10_codes populated ({non_null}/{len(df)} rows)",
            non_null > 0,
        ))

    # Medication extraction
    checks.append(_check("medication_names column present", "medication_names" in df.columns))
    if "medication_names" in df.columns:
        med_non_null = df["medication_names"].notna().sum()
        checks.append(_check(
            f"medication_names populated ({med_non_null}/{len(df)} rows)",
            med_non_null > 0,
        ))

    # DQ scores
    checks.append(_check("dq_score_pre present", "dq_score_pre" in df.columns))
    checks.append(_check("dq_score_post present", "dq_score_post" in df.columns))

    # Spot-check: rows with "E11" in diagnosis_text should have icd10_codes
    if "diagnosis_text" in df.columns and "icd10_codes" in df.columns:
        diabetes_rows = df[df["diagnosis_text"].str.contains("E11", na=False)]
        diabetes_coded = diabetes_rows["icd10_codes"].notna().sum()
        checks.append(_check(
            f"Diabetes rows (E11) have codes ({diabetes_coded}/{len(diabetes_rows)})",
            diabetes_coded == len(diabetes_rows) and len(diabetes_rows) > 0,
        ))

    return checks


def cleanup() -> None:
    if SCHEMA_FILE.exists():
        SCHEMA_FILE.unlink()
        print(f"\n  Cleaned up {SCHEMA_FILE.name}")


def main() -> int:
    if not FIXTURE_CSV.exists():
        print(f"FIXTURE MISSING: {FIXTURE_CSV}")
        return 1

    result = run_pipeline()
    print()
    checks = validate_output(result)

    passed = sum(checks)
    total = len(checks)
    print(f"\n=== Results: {passed}/{total} checks passed ===")

    cleanup()

    return 0 if all(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
