"""Unit tests for block_scaffolder.py (T018)."""

from __future__ import annotations

import ast
import textwrap
from unittest.mock import MagicMock, patch


VALID_BLOCK_SOURCE = textwrap.dedent("""\
    import logging
    import re
    import pandas as pd
    from src.blocks.base import Block

    logger = logging.getLogger(__name__)

    ICD10_PATTERN = re.compile(r"\\b([A-Z][0-9]{2}(?:\\.[0-9A-Z]{1,4})?)\\b", re.I)


    class ExtractIcd10Block(Block):
        name = "healthcare_test__extract_icd10"
        domain = "healthcare_test"
        description = "Extract ICD-10 codes from diagnosis_text"
        inputs = ["diagnosis_text"]
        outputs = ["icd10_codes"]

        def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
            df = df.copy()
            if "diagnosis_text" not in df.columns:
                df["icd10_codes"] = None
                return df
            df["icd10_codes"] = df["diagnosis_text"].apply(
                lambda t: ", ".join(ICD10_PATTERN.findall(str(t))) if pd.notna(t) else None
            )
            return df
""")

INVALID_BLOCK_SOURCE = "class Broken Block:\n    pass\n"
FENCED_SOURCE = f"```python\n{VALID_BLOCK_SOURCE}\n```"


def _make_mock_completion(content: str):
    resp = MagicMock()
    resp.choices = [MagicMock()]
    resp.choices[0].message.content = content
    return resp


@patch("src.ui.block_scaffolder.litellm.completion")
@patch("src.ui.block_scaffolder.get_orchestrator_llm", return_value="mock-model")
def test_valid_scaffold_returns_syntax_valid_true(mock_model, mock_completion):
    mock_completion.return_value = _make_mock_completion(VALID_BLOCK_SOURCE)
    from src.ui.block_scaffolder import generate_block_scaffold
    source, valid = generate_block_scaffold("healthcare_test", "Extract ICD-10 codes")
    assert valid is True
    ast.parse(source)


@patch("src.ui.block_scaffolder.litellm.completion")
@patch("src.ui.block_scaffolder.get_orchestrator_llm", return_value="mock-model")
def test_markdown_fences_stripped(mock_model, mock_completion):
    mock_completion.return_value = _make_mock_completion(FENCED_SOURCE)
    from src.ui.block_scaffolder import generate_block_scaffold
    source, valid = generate_block_scaffold("healthcare_test", "Extract ICD-10 codes")
    assert "```" not in source
    assert valid is True


@patch("src.ui.block_scaffolder.litellm.completion")
@patch("src.ui.block_scaffolder.get_orchestrator_llm", return_value="mock-model")
def test_malformed_source_returns_syntax_valid_false(mock_model, mock_completion):
    mock_completion.return_value = _make_mock_completion(INVALID_BLOCK_SOURCE)
    from src.ui.block_scaffolder import generate_block_scaffold
    source, valid = generate_block_scaffold("healthcare_test", "Extract ICD-10 codes")
    assert valid is False
    # no exception raised
    assert isinstance(source, str)


@patch("src.ui.block_scaffolder.litellm.completion")
@patch("src.ui.block_scaffolder.get_orchestrator_llm", return_value="mock-model")
def test_valid_source_class_inherits_from_block(mock_model, mock_completion):
    mock_completion.return_value = _make_mock_completion(VALID_BLOCK_SOURCE)
    from src.ui.block_scaffolder import generate_block_scaffold
    source, valid = generate_block_scaffold("healthcare_test", "Extract ICD-10 codes")
    assert valid is True
    assert "Block" in source


@patch("src.ui.block_scaffolder.litellm.completion", side_effect=Exception("LLM down"))
@patch("src.ui.block_scaffolder.get_orchestrator_llm", return_value="mock-model")
def test_llm_error_returns_syntax_valid_false_no_exception(mock_model, mock_completion):
    from src.ui.block_scaffolder import generate_block_scaffold
    source, valid = generate_block_scaffold("healthcare_test", "Extract ICD-10 codes")
    assert valid is False
    assert isinstance(source, str)
