import pandas as pd
from src.blocks.base import Block


class TYPE_CONVERSION_serving_size_unit_usda_sample_rawBlock(Block):
    name = "TYPE_CONVERSION_serving_size_unit_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform foodNutrients to serving_size_unit"
    inputs = ['foodNutrients']
    outputs = ['serving_size_unit']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        
        def extract_unit(nutrients):
            if pd.isna(nutrients):
                return None
            try:
                if isinstance(nutrients, str):
                    import ast
                    nutrients = ast.literal_eval(nutrients)
                
                if isinstance(nutrients, list) and len(nutrients) > 0:
                    for nutrient in nutrients:
                        if isinstance(nutrient, dict) and 'unitName' in nutrient:
                            return str(nutrient['unitName'])
            except (ValueError, SyntaxError, TypeError):
                pass
            return None
        
        df['serving_size_unit'] = df['foodNutrients'].apply(extract_unit).astype(str)
        
        return df