"""Agent 2 — Code Generator: LLM code generation + block registration."""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from src.agents.state import PipelineState, GeneratedBlock
from src.agents.prompts import CODEGEN_PROMPT, CODEGEN_RETRY_PROMPT
from src.agents.sandbox import execute_in_sandbox
from src.models.llm import call_llm, get_codegen_llm
from src.registry.block_registry import BlockRegistry

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATED_BLOCKS_DIR = PROJECT_ROOT / "src" / "blocks" / "generated"


def _clean_code_response(raw: str) -> str:
    """Strip markdown fences from LLM code responses."""
    code = raw.strip()
    if code.startswith("```python"):
        code = code[len("```python") :].strip()
    elif code.startswith("```"):
        code = code[3:].strip()
    if code.endswith("```"):
        code = code[:-3].strip()
    return code


def _determine_gap_type(gap: dict) -> str:
    """Determine the gap type based on gap characteristics."""
    source_col = gap.get("source_column")
    target_col = gap.get("target_column")
    source_type = gap.get("source_type", "")
    target_type = gap.get("target_type", "")

    if source_col is None:
        return "COLUMN_CREATE"

    if source_type != target_type:
        return "TYPE_CONVERSION"

    if source_col != target_col:
        return "COLUMN_RENAME"

    return "FORMAT_TRANSFORM"


def _get_domain_dir(domain: str) -> Path:
    """Get or create the domain-specific directory for generated blocks."""
    domain_dir = GENERATED_BLOCKS_DIR / domain
    domain_dir.mkdir(parents=True, exist_ok=True)
    return domain_dir


def generate_code_node(state: PipelineState) -> dict:
    """
    Agent 2: Generate transformation blocks for each registry miss.

    Calls LLM for code generation, validates in sandbox.
    """
    misses = state.get("registry_misses", [])
    retry_count = state.get("retry_count", 0)
    previous_blocks = state.get("generated_blocks", [])
    model = get_codegen_llm()
    domain = state.get("domain", "nutrition")
    dataset_name = Path(state.get("source_path", "unknown")).stem

    generated: list[GeneratedBlock] = []

    if retry_count > 0 and previous_blocks:
        failed = [b for b in previous_blocks if not b.get("validation_passed")]
        misses = [
            gap
            for gap in state.get("registry_misses", [])
            if any(
                b.get("block_name", "").replace("Block", "")
                == gap.get("target_column", "")
                for b in failed
            )
        ]
        generated = [b for b in previous_blocks if b.get("validation_passed")]

    column_mapping = state.get("column_mapping", {})

    for gap in misses:
        target_col = gap.get("target_column", "")
        target_type = gap.get("target_type", "string")
        source_col = gap.get("source_column")
        source_type = gap.get("source_type", "string")
        sample_values = gap.get("sample_values", [])

        # The runner applies column_mapping before executing any blocks, so by
        # the time a generated block runs, source columns may have been renamed.
        # Resolve to the post-mapping name so the generated code uses the right key.
        effective_source_col = column_mapping.get(source_col, source_col) if source_col else None

        gap_type = _determine_gap_type(gap)
        block_name = f"{gap_type}_{target_col}_{dataset_name}"

        logger.info(f"Generating block: {block_name} (attempt {retry_count + 1})")

        previous_code = None
        previous_error = None
        if retry_count > 0:
            prev = next(
                (
                    b
                    for b in previous_blocks
                    if b.get("block_name", "").replace("Block", "") == target_col
                ),
                None,
            )
            if prev:
                previous_code = prev.get("block_code", "")
                previous_error = prev.get("validation_error", "Unknown error")

        if previous_code and previous_error:
            prompt = CODEGEN_RETRY_PROMPT.format(
                error=previous_error,
                previous_code=previous_code,
                target_column=target_col,
                target_type=target_type,
                source_column=effective_source_col or "N/A",
                source_type=source_type,
                sample_values=sample_values,
                domain=domain,
            )
        else:
            prompt = CODEGEN_PROMPT.format(
                target_column=target_col,
                target_type=target_type,
                source_column=effective_source_col or "N/A",
                source_type=source_type,
                sample_values=sample_values,
                domain=domain,
                dataset_name=dataset_name,
                block_name=block_name,
                description=f"Transform {effective_source_col or 'new column'} to {target_col}",
                input_cols=f"['{effective_source_col}']" if effective_source_col else "[]",
                output_cols=f"['{target_col}']",
            )

        try:
            raw_code = call_llm(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            code = _clean_code_response(raw_code)

            validation = _validate_block_code(code, block_name, effective_source_col, target_col)

            block_entry: GeneratedBlock = {
                "block_name": f"{block_name}Block",
                "block_code": code,
                "file_path": None,  # set by register_blocks_node after disk write
                "target_column": target_col,
                "source_column": source_col,
                "gap_type": gap_type,
                "validation_passed": validation["passed"],
                "validation_error": validation.get("error"),
            }

            if not validation["passed"]:
                logger.warning(
                    f"Validation failed for {block_name}: {validation.get('error')}"
                )
            else:
                logger.info(f"Block {block_name} generated and validated successfully")

            generated.append(block_entry)

        except Exception as e:
            logger.error(f"Block generation failed for {target_col}: {e}")
            generated.append(
                {
                    "block_name": f"{block_name}Block",
                    "block_code": "",
                    "file_path": None,
                    "target_column": target_col,
                    "source_column": source_col,
                    "gap_type": gap_type,
                    "validation_passed": False,
                    "validation_error": str(e),
                }
            )

    return {
        "generated_blocks": generated,
        "retry_count": retry_count + 1,
    }


def _runtime_validate_block(code: str, source_col: str | None, target_col: str) -> dict:
    """
    Run the generated Block class against a minimal test DataFrame in a subprocess.

    Only runs for blocks with a known source column (TYPE_CONVERSION, FORMAT_TRANSFORM,
    COLUMN_RENAME). COLUMN_CREATE blocks (source_col is None) are skipped because they
    may legitimately access other pipeline columns that aren't present in the minimal
    test DataFrame, causing false failures.

    Returns {"passed": bool, "error": str | None}.
    """
    if source_col is None:
        # Can't construct a meaningful test DataFrame without knowing which
        # pipeline columns will be present at runtime; safety check is enough.
        from src.agents.sandbox import is_code_safe
        safe, reason = is_code_safe(code)
        if not safe:
            return {"passed": False, "error": f"Safety check failed: {reason}"}
        return {"passed": True, "error": None}

    from src.agents.sandbox import is_code_safe

    safe, reason = is_code_safe(code)
    if not safe:
        return {"passed": False, "error": f"Safety check failed: {reason}"}

    # Extract class name from code (e.g. "class FooBlock(Block):" → "FooBlock")
    match = re.search(r"class\s+(\w+Block)\s*\(", code)
    if not match:
        return {"passed": False, "error": "Could not determine Block class name"}
    class_name = match.group(1)

    # Build a minimal DataFrame with the source column containing null values
    cols = {source_col: [None, None]}

    test_script = f"""
import sys
sys.path.insert(0, {repr(str(PROJECT_ROOT))})
import pandas as pd

{code}

block = {class_name}()
test_df = pd.DataFrame({repr(cols)})
result = block.run(test_df)
assert isinstance(result, pd.DataFrame), "run() must return a DataFrame"
print("PASS")
"""

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(test_script)
            script_path = f.name

        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT)

        proc = subprocess.run(
            ["python3", script_path],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
        Path(script_path).unlink(missing_ok=True)

        if proc.returncode != 0:
            error = proc.stderr.strip() or proc.stdout.strip()
            return {"passed": False, "error": f"Runtime error: {error}"}
        return {"passed": True, "error": None}

    except subprocess.TimeoutExpired:
        Path(script_path).unlink(missing_ok=True)
        return {"passed": False, "error": "Runtime validation timed out (10s)"}
    except Exception as e:
        return {"passed": False, "error": f"Runtime validation failed: {e}"}


def _validate_block_code(
    code: str, block_name: str, source_col: str | None = None, target_col: str = ""
) -> dict:
    """Validate generated block code: syntax check then runtime execution."""
    try:
        compile(code, block_name, "exec")

        if "class" not in code or "Block" not in code:
            return {"passed": False, "error": "Block class not found in code"}

        if "def run(" not in code:
            return {"passed": False, "error": "run() method not found in Block"}

    except SyntaxError as e:
        return {"passed": False, "error": f"Syntax error: {str(e)}"}
    except Exception as e:
        return {"passed": False, "error": f"Validation error: {str(e)}"}

    return _runtime_validate_block(code, source_col, target_col)


def validate_code_node(state: PipelineState) -> dict:
    """Check if all generated blocks passed validation."""
    return {}


def register_blocks_node(state: PipelineState) -> dict:
    """Save validated blocks to the generated blocks directory."""
    generated = state.get("generated_blocks", [])
    domain = state.get("domain", "nutrition")
    domain_dir = _get_domain_dir(domain)

    saved_blocks = []
    for block in generated:
        if not block.get("validation_passed"):
            saved_blocks.append(block)
            continue

        block_name = block.get("block_name", "")
        block_code = block.get("block_code", "")

        file_name = f"{block_name}.py"
        file_path = domain_dir / file_name

        try:
            file_path.write_text(block_code)
            block["file_path"] = str(file_path)
            logger.info(f"Registered block: {block_name} -> {file_path}")
        except Exception as e:
            logger.error(f"Failed to save block {block_name}: {e}")
            block["validation_passed"] = False
            block["validation_error"] = str(e)

        saved_blocks.append(block)

    BlockRegistry.instance().refresh()
    return {"generated_blocks": saved_blocks}
