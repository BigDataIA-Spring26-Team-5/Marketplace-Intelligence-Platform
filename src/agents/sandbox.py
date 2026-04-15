"""Sandboxed code execution for Agent 2 generated functions."""

from __future__ import annotations

import json
import logging
import re
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Patterns that are forbidden in generated code
BANNED_PATTERNS = [
    r"\bimport\s+os\b",
    r"\bimport\s+sys\b",
    r"\bimport\s+subprocess\b",
    r"\bfrom\s+os\b",
    r"\bfrom\s+sys\b",
    r"\bfrom\s+subprocess\b",
    r"\bopen\s*\(",
    r"\beval\s*\(",
    r"\bexec\s*\(",
    r"\b__import__\s*\(",
    r"\bcompile\s*\(",
    r"\bglobals\s*\(",
    r"\bgetattr\s*\(",
]

BANNED_RE = re.compile("|".join(BANNED_PATTERNS))


def is_code_safe(code: str) -> tuple[bool, str]:
    """
    Static analysis: check for banned patterns in generated code.

    Strips single-line comments before scanning so that LLM-generated
    comments containing words like 'null' don't trip patterns that are
    only meaningful as actual code tokens.

    Returns (is_safe, reason).
    """
    scannable = re.sub(r"#[^\n]*", "", code)
    match = BANNED_RE.search(scannable)
    if match:
        return False, f"Banned pattern found: {match.group()}"
    return True, "OK"


def execute_in_sandbox(
    function_code: str,
    function_name: str,
    sample_values: list,
    target_type: str,
    timeout: int = 5,
) -> dict:
    """
    Execute a generated function against sample values in a subprocess.

    Returns:
        {
            "passed": bool,
            "outputs": {input_val: output_val, ...},
            "error": str | None
        }
    """
    # Safety check first
    safe, reason = is_code_safe(function_code)
    if not safe:
        return {
            "passed": False,
            "outputs": {},
            "error": f"Safety check failed: {reason}",
        }

    # Build test script
    test_script = f"""
import json
import sys

{function_code}

sample_values = {repr(sample_values)}
results = {{}}
errors = []

for val in sample_values:
    try:
        result = {function_name}(val)
        results[str(val)] = result
    except Exception as e:
        errors.append(f"Input {{val}}: {{str(e)}}")

# Type checking
type_map = {{"string": str, "float": (int, float), "integer": int, "boolean": bool}}
expected_type = type_map.get("{target_type}")

type_ok = True
for val, result in results.items():
    if result is not None and expected_type:
        if not isinstance(result, expected_type):
            type_ok = False
            errors.append(f"Type mismatch for {{val}}: got {{type(result).__name__}}, expected {target_type}")

output = {{
    "passed": len(errors) == 0 and type_ok,
    "outputs": {{k: str(v) if v is not None else None for k, v in results.items()}},
    "error": "; ".join(errors) if errors else None,
}}
print(json.dumps(output))
"""

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(test_script)
            script_path = f.name

        result = subprocess.run(
            ["python3", script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        # Cleanup
        Path(script_path).unlink(missing_ok=True)

        if result.returncode != 0:
            return {
                "passed": False,
                "outputs": {},
                "error": f"Execution error: {result.stderr.strip()}",
            }

        return json.loads(result.stdout.strip())

    except subprocess.TimeoutExpired:
        Path(script_path).unlink(missing_ok=True)
        return {
            "passed": False,
            "outputs": {},
            "error": "Execution timed out (5s limit)",
        }
    except json.JSONDecodeError:
        return {
            "passed": False,
            "outputs": {},
            "error": f"Invalid output: {result.stdout[:200]}",
        }
    except Exception as e:
        return {"passed": False, "outputs": {}, "error": str(e)}
