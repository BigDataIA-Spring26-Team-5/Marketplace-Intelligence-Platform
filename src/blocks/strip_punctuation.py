"""Replace non-alphanumeric characters (except spaces) with spaces in name fields."""

import re

import pandas as pd
from src.blocks.base import Block


class StripPunctuationBlock(Block):
    name = "strip_punctuation"
    domain = "all"
    description = "Replace non-alphanumeric characters with spaces in product_name and brand_name"
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
                                (cleaned := re.sub(r"[^\w\s]", " ", str(v))) or str(v)
                            )
                        )
                    )
                    .str.replace(r"\s+", " ", regex=True)
                    .str.strip()
                    .replace("nan", pd.NA)
                )
        return df
