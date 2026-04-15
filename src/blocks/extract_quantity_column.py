"""Extract size/quantity from product name into a separate 'sizes' column."""

import re

import pandas as pd
from src.blocks.base import Block

SIZE_PATTERN = re.compile(
    r"(\d+\.?\d*)\s*(oz|g|mg|kg|lb|lbs|ml|l|fl\s*oz|gal|ct|count|pk|pack)\b",
    re.IGNORECASE,
)


class ExtractQuantityColumnBlock(Block):
    name = "extract_quantity_column"
    domain = "nutrition"
    description = "Extract size/quantity from product_name into a separate sizes column"
    inputs = ["product_name"]
    outputs = ["product_name", "sizes"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        if "product_name" not in df.columns:
            return df

        sizes = []
        cleaned_names = []
        for name in df["product_name"]:
            name = str(name) if not isinstance(name, str) else name
            if name == "nan":
                sizes.append(pd.NA)
                cleaned_names.append(name)
                continue
            matches = SIZE_PATTERN.findall(name)
            if matches:
                size_str = "; ".join(f"{val}{unit}" for val, unit in matches)
                sizes.append(size_str)
                cleaned = SIZE_PATTERN.sub("", name).strip()
                cleaned = re.sub(r"\s+", " ", cleaned).strip(", ")
                cleaned_names.append(cleaned if cleaned else name)
            else:
                sizes.append(pd.NA)
                cleaned_names.append(name)

        df["sizes"] = sizes
        df["product_name"] = cleaned_names
        return df
