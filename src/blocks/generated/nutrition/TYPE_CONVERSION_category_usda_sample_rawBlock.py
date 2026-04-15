import pandas as pd
from src.blocks.base import Block


class TYPE_CONVERSION_category_usda_sample_rawBlock(Block):
    name = "TYPE_CONVERSION_category_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform dataType to category"
    inputs = ['dataType']
    outputs = ['category']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        
        # Convert object to string type
        if 'dataType' in df.columns:
            df['category'] = df['dataType'].astype(str)
        else:
            df['category'] = None
            
        return df