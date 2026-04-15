"""Strip legal suffixes, frequency-based noise words, and known aliases."""

import re

import pandas as pd
from src.blocks.base import Block

LEGAL_SUFFIXES = [
    r"\b(inc|llc|ltd|corp|co|company|corporation|incorporated|limited)\b\.?",
]

NOISE_PATTERNS = re.compile(
    "|".join(LEGAL_SUFFIXES),
    re.IGNORECASE,
)


class RemoveNoiseWordsBlock(Block):
    name = "remove_noise_words"
    domain = "all"
    description = "Remove legal suffixes and noise words from product_name and brand_name"
    inputs = ["product_name", "brand_name"]
    outputs = ["product_name", "brand_name"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        target_cols = ["product_name", "brand_name"]
        for col in target_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).where(df[col].astype(str) != "nan", pd.NA)
                df[col] = (
                    df[col]
                    .apply(
                        lambda v: (
                            v
                            if v == "nan"
                            else (
                                (cleaned := NOISE_PATTERNS.sub("", str(v)).strip())
                                or str(v)
                            )
                        )
                    )
                    .replace("nan", pd.NA)
                )
        return df
