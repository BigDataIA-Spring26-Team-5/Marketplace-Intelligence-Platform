"""Extract known medication names from clinical medication field."""

import logging
import re

import pandas as pd

from src.blocks.base import Block

logger = logging.getLogger(__name__)

# Common medication stems — covers generics and common brand variants
MEDICATION_PATTERNS = {
    "metformin": re.compile(r"\bmetformin\b", re.I),
    "lisinopril": re.compile(r"\blisinopril\b", re.I),
    "atorvastatin": re.compile(r"\batorvastatin\b", re.I),
    "omeprazole": re.compile(r"\bomeprazole\b", re.I),
    "amlodipine": re.compile(r"\bamlodipine\b", re.I),
    "levothyroxine": re.compile(r"\blevothyroxine\b", re.I),
    "albuterol": re.compile(r"\balbuterol\b", re.I),
    "gabapentin": re.compile(r"\bgabapentin\b", re.I),
    "losartan": re.compile(r"\blosartan\b", re.I),
    "metoprolol": re.compile(r"\bmetoprolol\b", re.I),
    "aspirin": re.compile(r"\baspirin\b", re.I),
    "warfarin": re.compile(r"\bwarfarin\b", re.I),
    "furosemide": re.compile(r"\bfurosemide\b", re.I),
    "prednisone": re.compile(r"\bprednisone\b", re.I),
    "insulin": re.compile(r"\binsulin\b", re.I),
    "sertraline": re.compile(r"\bsertraline\b", re.I),
    "escitalopram": re.compile(r"\bescitalopram\b", re.I),
    "azithromycin": re.compile(r"\bazithromycin\b", re.I),
    "ciprofloxacin": re.compile(r"\bciprofloxacin\b", re.I),
    "ibuprofen": re.compile(r"\bibuprofen\b", re.I),
}


class ExtractMedicationsBlock(Block):
    name = "healthcare_test__extract_medications"
    domain = "healthcare_test"
    description = "Extract known medication names from medications field"
    inputs = ["medications"]
    outputs = ["medication_names"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()

        if "medications" not in df.columns:
            logger.warning("medications column missing — medication_names set to NA")
            df["medication_names"] = pd.NA
            return df

        def extract(text: str) -> str | None:
            if not isinstance(text, str) or not text.strip():
                return None
            found = [name for name, pat in MEDICATION_PATTERNS.items() if pat.search(text)]
            return ", ".join(found) if found else None

        df["medication_names"] = df["medications"].apply(extract)
        found = df["medication_names"].notna().sum()
        logger.info("Medications: extracted from %d/%d rows (%.1f%%)", found, len(df), found / len(df) * 100)
        return df
