"""Strip leading/trailing whitespace from all string columns."""

import pandas as pd
from src.blocks.base import Block


class StripWhitespaceBlock(Block):
    name = "strip_whitespace"
    domain = "all"
    description = "Strip leading/trailing whitespace from all string columns"
    inputs = ["string columns"]
    outputs = ["string columns (cleaned)"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            df[col] = df[col].str.strip().replace("", pd.NA)
        return df
