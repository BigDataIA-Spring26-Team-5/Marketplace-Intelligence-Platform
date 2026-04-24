"""Extract ICD-10 codes from clinical diagnosis text."""

import logging
import re

import pandas as pd

from src.blocks.base import Block

logger = logging.getLogger(__name__)

# ICD-10 format: letter + 2 digits, optional dot + 1-4 alphanumeric chars
# e.g. E11.9, I10, J45.909, F32.1
ICD10_RE = re.compile(r"\b([A-Z][0-9]{2}(?:\.[0-9A-Z]{1,4})?)\b")


class ExtractIcd10CodesBlock(Block):
    name = "healthcare_test__extract_icd10_codes"
    domain = "healthcare_test"
    description = "Extract ICD-10 diagnosis codes from diagnosis_text field"
    inputs = ["diagnosis_text"]
    outputs = ["icd10_codes"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()

        if "diagnosis_text" not in df.columns:
            logger.warning("diagnosis_text column missing — icd10_codes set to NA")
            df["icd10_codes"] = pd.NA
            return df

        def extract(text: str) -> str | None:
            if not isinstance(text, str) or not text.strip():
                return None
            codes = ICD10_RE.findall(text)
            return ", ".join(dict.fromkeys(codes)) if codes else None

        df["icd10_codes"] = df["diagnosis_text"].apply(extract)
        found = df["icd10_codes"].notna().sum()
        logger.info("ICD-10 codes: extracted from %d/%d rows (%.1f%%)", found, len(df), found / len(df) * 100)
        return df
