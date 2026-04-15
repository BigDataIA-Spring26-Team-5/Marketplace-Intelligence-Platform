"""Block template for Agent 2 - Use this as base for generating new blocks."""

import pandas as pd
from src.blocks.base import Block


class {BlockName}Block(Block):
    """
    Auto-generated block for schema transformation.
    
    Gap Type: {GapType}
    Source Column: {SourceColumn}
    Target Column: {TargetColumn}
    Domain: {Domain}
    """
    
    name = "{BlockName}"
    domain = "{Domain}"
    description = "{Description}"
    inputs = [{InputColumns}]
    outputs = [{OutputColumns}]
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # TODO: Implement transformation logic
        # Gap Type: {GapType}
        # Example: df["{TargetColumn}"] = df["{SourceColumn}"].astype(str)
        return df