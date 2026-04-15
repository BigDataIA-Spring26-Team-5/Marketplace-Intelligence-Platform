import pandas as pd
from src.blocks.base import Block


class TYPE_CONVERSION_data_source_usda_sample_rawBlock(Block):
    name = "TYPE_CONVERSION_data_source_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform fdcId to data_source"
    inputs = ['fdcId']
    outputs = ['data_source']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        
        if 'fdcId' in df.columns:
            # Convert int64 to string, handling NaN/None values
            df['data_source'] = df['fdcId'].astype('Int64').astype('string')
        else:
            # If source column doesn't exist, create target column with null values
            df['data_source'] = pd.NA
        
        return df