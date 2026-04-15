import pandas as pd
from src.blocks.base import Block


class TYPE_CONVERSION_serving_size_usda_sample_rawBlock(Block):
    name = "TYPE_CONVERSION_serving_size_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform foodNutrients to serving_size"
    inputs = ['foodNutrients']
    outputs = ['serving_size']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        
        # Initialize serving_size column with NaN
        df['serving_size'] = float('nan')
        
        # Extract serving size from foodNutrients if available
        if 'foodNutrients' in df.columns:
            for idx, nutrients in enumerate(df['foodNutrients']):
                try:
                    if isinstance(nutrients, str):
                        # Parse string representation of list
                        import ast
                        nutrients = ast.literal_eval(nutrients)
                    
                    if isinstance(nutrients, list):
                        # Look for serving size nutrient (typically number '208' for Energy)
                        for nutrient in nutrients:
                            if isinstance(nutrient, dict):
                                # Check if this is a serving size related nutrient
                                # In USDA data, serving size might be derived from energy or other nutrients
                                nutrient_num = str(nutrient.get('number', ''))
                                nutrient_amount = nutrient.get('amount')
                                
                                # Use energy (208) as proxy for serving size calculation
                                # Or look for specific serving size nutrient if available
                                if nutrient_num == '208' and nutrient_amount is not None:
                                    # Convert to float, assuming amount is numeric
                                    df.loc[idx, 'serving_size'] = float(nutrient_amount)
                                    break
                except (ValueError, TypeError, SyntaxError, KeyError):
                    # Keep NaN if parsing fails
                    continue
        
        # Ensure float type with safe conversion
        df['serving_size'] = pd.to_numeric(df['serving_size'], errors='coerce')
        
        return df