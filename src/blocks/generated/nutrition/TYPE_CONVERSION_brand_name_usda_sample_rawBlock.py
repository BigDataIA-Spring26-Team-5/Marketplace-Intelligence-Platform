import pandas as pd
from src.blocks.base import Block


class TYPE_CONVERSION_brand_name_usda_sample_rawBlock(Block):
    name = "TYPE_CONVERSION_brand_name_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform brand_owner to brand_name"
    inputs = ['brand_owner']
    outputs = ['brand_name']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # Convert object type to string type
        df['brand_name'] = df['brand_owner'].astype(str)
        return df