"""Template for Agent 2 to generate new transformation blocks."""

from src.blocks.base import Block


class GeneratedBlock(Block):
    """
    Generated block template for schema transformation.

    Agent 2 fills in the run() method based on the schema gap type:
    - Column mapping: rename column
    - Column deletion: drop column
    - Type conversion: cast to target type
    - New column creation: initialize with default value
    """

    name = "generated_block"
    domain = "nutrition"  # Set by Agent 2 based on domain
    description = "Auto-generated transformation block"
    inputs = []
    outputs = []

    def run(self, df, config=None):
        df = df.copy()
        # TODO: Agent 2 fills in transformation logic
        return df
