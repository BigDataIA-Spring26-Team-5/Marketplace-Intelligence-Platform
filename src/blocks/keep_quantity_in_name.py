"""Keep quantity embedded in product name (no-op for pricing domain)."""

import pandas as pd
from src.blocks.base import Block


class KeepQuantityInNameBlock(Block):
    name = "keep_quantity_in_name"
    domain = "pricing"
    description = "Preserve quantity embedded in product_name (no-op for pricing domain)"
    inputs = ["product_name"]
    outputs = ["product_name"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        # Intentional no-op: in pricing domain, quantity stays in the name
        return df
