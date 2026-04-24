"""Unit tests for healthcare_test custom blocks."""

from __future__ import annotations

import pandas as pd
import pytest

from domain_packs.healthcare_test.custom_blocks.extract_icd10_codes import ExtractIcd10CodesBlock
from domain_packs.healthcare_test.custom_blocks.extract_medications import ExtractMedicationsBlock


# --- ExtractIcd10CodesBlock ---

class TestExtractIcd10CodesBlock:
    def setup_method(self):
        self.block = ExtractIcd10CodesBlock()

    def test_block_metadata(self):
        assert self.block.name == "healthcare_test__extract_icd10_codes"
        assert self.block.domain == "healthcare_test"
        assert "diagnosis_text" in self.block.inputs
        assert "icd10_codes" in self.block.outputs

    def test_extracts_single_code(self):
        df = pd.DataFrame({"diagnosis_text": ["E11.9 Type 2 diabetes mellitus"]})
        out = self.block.run(df)
        assert out["icd10_codes"].iloc[0] == "E11.9"

    def test_extracts_multiple_codes(self):
        df = pd.DataFrame({"diagnosis_text": ["I10 Essential hypertension E78.5 Hyperlipidemia"]})
        out = self.block.run(df)
        codes = out["icd10_codes"].iloc[0]
        assert "I10" in codes
        assert "E78.5" in codes

    def test_deduplicates_repeated_codes(self):
        df = pd.DataFrame({"diagnosis_text": ["E11.9 diabetes E11.9 followup"]})
        out = self.block.run(df)
        codes = out["icd10_codes"].iloc[0].split(", ")
        assert codes.count("E11.9") == 1

    def test_null_on_empty_text(self):
        df = pd.DataFrame({"diagnosis_text": ["", None, "  "]})
        out = self.block.run(df)
        assert out["icd10_codes"].isna().all() or (out["icd10_codes"] == "").all() or out["icd10_codes"].isna().sum() >= 2

    def test_missing_column_returns_na(self):
        df = pd.DataFrame({"other_col": ["some data"]})
        out = self.block.run(df)
        assert "icd10_codes" in out.columns
        assert out["icd10_codes"].isna().all()

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"diagnosis_text": ["E11.9 diabetes"]})
        original_cols = list(df.columns)
        self.block.run(df)
        assert list(df.columns) == original_cols

    def test_audit_entry(self):
        entry = self.block.audit_entry(100, 100)
        assert entry["block"] == self.block.name
        assert entry["rows_in"] == 100
        assert entry["rows_out"] == 100

    def test_fixture_csv_extraction(self):
        from pathlib import Path
        csv = Path(__file__).resolve().parent.parent / "fixtures" / "healthcare_sample.csv"
        if not csv.exists():
            pytest.skip("healthcare_sample.csv fixture not found")
        df = pd.read_csv(csv)
        out = self.block.run(df)
        assert "icd10_codes" in out.columns
        non_null = out["icd10_codes"].notna().sum()
        assert non_null == len(df), f"Only {non_null}/{len(df)} rows got codes"


# --- ExtractMedicationsBlock ---

class TestExtractMedicationsBlock:
    def setup_method(self):
        self.block = ExtractMedicationsBlock()

    def test_block_metadata(self):
        assert self.block.name == "healthcare_test__extract_medications"
        assert self.block.domain == "healthcare_test"
        assert "medications" in self.block.inputs
        assert "medication_names" in self.block.outputs

    def test_extracts_known_medication(self):
        df = pd.DataFrame({"medications": ["metformin 500mg twice daily"]})
        out = self.block.run(df)
        assert "metformin" in out["medication_names"].iloc[0]

    def test_extracts_multiple_medications(self):
        df = pd.DataFrame({"medications": ["metformin 500mg lisinopril 10mg"]})
        out = self.block.run(df)
        meds = out["medication_names"].iloc[0]
        assert "metformin" in meds
        assert "lisinopril" in meds

    def test_case_insensitive(self):
        df = pd.DataFrame({"medications": ["METFORMIN 1000mg Atorvastatin 40mg"]})
        out = self.block.run(df)
        meds = out["medication_names"].iloc[0]
        assert "metformin" in meds
        assert "atorvastatin" in meds

    def test_null_on_empty(self):
        df = pd.DataFrame({"medications": [None, "", "  "]})
        out = self.block.run(df)
        assert out["medication_names"].isna().sum() >= 2

    def test_null_on_unknown_drug(self):
        df = pd.DataFrame({"medications": ["somenoveldrug 200mg"]})
        out = self.block.run(df)
        assert out["medication_names"].iloc[0] is None

    def test_missing_column_returns_na(self):
        df = pd.DataFrame({"other_col": ["data"]})
        out = self.block.run(df)
        assert "medication_names" in out.columns
        assert out["medication_names"].isna().all()

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"medications": ["metformin 500mg"]})
        original_cols = list(df.columns)
        self.block.run(df)
        assert list(df.columns) == original_cols

    def test_fixture_csv_extraction(self):
        from pathlib import Path
        csv = Path(__file__).resolve().parent.parent / "fixtures" / "healthcare_sample.csv"
        if not csv.exists():
            pytest.skip("healthcare_sample.csv fixture not found")
        df = pd.read_csv(csv)
        out = self.block.run(df)
        assert "medication_names" in out.columns
        non_null = out["medication_names"].notna().sum()
        assert non_null > 0, "No medication names extracted from fixture"
