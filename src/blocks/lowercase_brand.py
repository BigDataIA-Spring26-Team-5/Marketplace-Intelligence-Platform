"""Normalize brand name casing to lowercase."""

import pandas as pd
from src.blocks.base import Block


class LowercaseBrandBlock(Block):
    name = "lowercase_brand"
    domain = "all"
    description = "Normalize brand_name to lowercase"
    inputs = ["brand_name"]
    outputs = ["brand_name"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        if "brand_name" in df.columns:
            df["brand_name"] = df["brand_name"].astype(str).str.lower().replace("nan", pd.NA)
        return df
