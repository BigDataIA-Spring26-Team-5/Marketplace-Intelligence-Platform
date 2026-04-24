"""Generate a Python Block subclass scaffold from a plain-language extraction description."""

from __future__ import annotations

import ast
import logging
import re
from pathlib import Path

import litellm

from src.models.llm import get_orchestrator_llm

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"

_FENCE_RE = re.compile(r"```(?:python)?\s*([\s\S]*?)```", re.MULTILINE)


def _load_example_block() -> str:
    """Return extract_allergens.py as a few-shot example."""
    p = DOMAIN_PACKS_DIR / "nutrition" / "custom_blocks" / "extract_allergens.py"
    try:
        return p.read_text()
    except Exception:
        return ""


def _strip_fences(source: str) -> str:
    """Strip markdown code fences if present."""
    m = _FENCE_RE.search(source)
    if m:
        return m.group(1).strip()
    return source.strip()


def generate_block_scaffold(
    domain_name: str,
    extraction_description: str,
) -> tuple[str, bool]:
    """Generate a Python Block subclass scaffold.

    Returns:
        (python_source, syntax_valid) — syntax_valid is False if ast.parse() fails.
        On parse failure the source includes a leading comment explaining the issue.
    """
    example = _load_example_block()

    prompt = f"""You are a Python code generator for an ETL pipeline.

Generate a single Python class that subclasses `Block` to perform the following extraction:

{extraction_description}

## Block Base Class Contract
```python
class Block:
    name: str = "unnamed"          # snake_case block identifier
    domain: str = "all"            # domain this block belongs to
    description: str = ""
    inputs: list[str] = []         # column names this block reads
    outputs: list[str] = []        # column names this block produces

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        \"\"\"Transform the dataframe. Must return the modified dataframe.\"\"\"
        ...

    def audit_entry(self, rows_in: int, rows_out: int, extra: dict | None = None) -> dict:
        # Do NOT override — inherited from Block base class
        ...
```

## Few-Shot Example: extract_allergens.py
```python
{example}
```

## Generation Rules
- Class name: `Extract<Noun>Block` (PascalCase noun derived from the extraction goal)
- Block name attribute: `{domain_name}__extract_<noun>` (lowercase, double underscore prefix)
- Block domain attribute: `"{domain_name}"`
- Import `re`, `logging`, `pandas as pd`, and `from src.blocks.base import Block`
- Compile regex patterns at module level (not inside the function)
- The `run()` method must:
  1. Copy the dataframe: `df = df.copy()`
  2. Handle missing columns gracefully (check `if col not in df.columns`)
  3. Return the modified dataframe
- Keep the code focused and concise — no unrelated logic

Return ONLY the Python source code. Do NOT include markdown fences or any explanation.
"""

    try:
        model = get_orchestrator_llm()
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_source = response.choices[0].message.content or ""
    except Exception as exc:
        logger.error("Block scaffolder LLM call failed: %s", exc)
        error_comment = f"# ERROR: LLM call failed — {exc}\n"
        return error_comment, False

    source = _strip_fences(raw_source)

    try:
        ast.parse(source)
        return source, True
    except SyntaxError as exc:
        logger.warning("Generated scaffold has syntax error: %s", exc)
        annotated = f"# SYNTAX ERROR: {exc}\n{source}"
        return annotated, False
