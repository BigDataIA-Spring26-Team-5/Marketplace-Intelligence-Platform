"""FDA Big-9 allergen keyword scan from ingredients field."""

import logging
import re

import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)

# FDA Big-9 allergens
BIG_9_ALLERGENS = [
    "milk", "egg", "fish", "shellfish", "tree nut",
    "peanut", "wheat", "soybean", "sesame",
]

# Expanded patterns to catch common variants
ALLERGEN_PATTERNS = {
    "milk": re.compile(r"\b(milk|dairy|lactose|casein|whey|cream|butter)\b", re.I),
    "egg": re.compile(r"\b(egg|albumin|lysozyme|mayonnaise)\b", re.I),
    "fish": re.compile(r"\b(fish|cod|salmon|tuna|anchov|bass|tilapia)\b", re.I),
    "shellfish": re.compile(r"\b(shellfish|shrimp|crab|lobster|crawfish|prawn)\b", re.I),
    "tree nut": re.compile(r"\b(almond|cashew|walnut|pecan|pistachio|macadamia|hazelnut|brazil\s*nut)\b", re.I),
    "peanut": re.compile(r"\b(peanut)\b", re.I),
    "wheat": re.compile(r"\b(wheat|flour|gluten|semolina|durum|spelt)\b", re.I),
    "soybean": re.compile(r"\b(soy|soybean|soya|tofu|edamame|lecithin)\b", re.I),
    "sesame": re.compile(r"\b(sesame|tahini)\b", re.I),
}


class ExtractAllergensBlock(Block):
    name = "extract_allergens"
    domain = "nutrition"
    description = "Scan ingredients for FDA Big-9 allergens using keyword patterns"
    inputs = ["ingredients"]
    outputs = ["allergens"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()

        def scan_allergens(text: str) -> str | None:
            if not isinstance(text, str) or text.strip() == "" or text == "nan":
                return None
            found = []
            for allergen, pattern in ALLERGEN_PATTERNS.items():
                if pattern.search(text):
                    found.append(allergen)
            return ", ".join(sorted(found)) if found else ""

        def _get_scan_text(row) -> str:
            return str(row.get("ingredients") or row.get("recall_reason") or "")

        if "ingredients" not in df.columns and "recall_reason" not in df.columns:
            df["allergens"] = pd.NA
            return df

        df["allergens"] = df.apply(lambda row: scan_allergens(_get_scan_text(row)), axis=1)
        detected = df["allergens"].notna().sum()
        logger.info(f"Allergens: detected in {detected}/{len(df)} rows ({detected/len(df)*100:.1f}%)")
        return df
