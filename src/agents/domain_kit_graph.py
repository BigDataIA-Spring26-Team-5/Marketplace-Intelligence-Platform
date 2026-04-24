"""LangGraph graphs for the Agentic Domain Kit Builder.

Two graphs:
  DomainKitGraph (8 nodes):
    analyze_csv → generate_enrichment_rules → validate_enrichment_rules →
    (revise_enrichment_rules?) → generate_prompt_examples → generate_block_sequence →
    hitl_review → commit_to_disk

  ScaffoldGraph (5 nodes):
    generate_scaffold → validate_syntax → (fix_scaffold?) → hitl_review → save_to_custom_blocks

run_kit_step() and run_scaffold_step() mirror the run_step() signature from graph.py.
"""

from __future__ import annotations

import ast
import csv
import io
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from src.agents.domain_kit_prompts import (
    build_block_sequence_prompt,
    build_enrichment_rules_fix_prompt,
    build_enrichment_rules_prompt,
    build_prompt_examples_prompt,
    build_scaffold_fix_prompt,
    build_scaffold_generate_prompt,
)
from src.models.llm import call_llm_json, get_orchestrator_llm

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"

_FENCE_RE = re.compile(r"```(?:\w+)?\s*([\s\S]*?)```", re.MULTILINE)


# ---------------------------------------------------------------------------
# State types
# ---------------------------------------------------------------------------


class DomainKitState(TypedDict, total=False):
    """LangGraph state for the domain pack generation agent."""

    # Inputs (set by caller before first node)
    domain_name: str
    description: str
    csv_content: str

    # analyze_csv outputs
    csv_headers: list
    csv_sample_table: str

    # generate_enrichment_rules / revise_enrichment_rules outputs
    enrichment_rules_yaml: str

    # validate_enrichment_rules outputs
    enrichment_fields: list
    validation_errors: list
    retry_count: int

    # generate_prompt_examples output
    prompt_examples_yaml: str

    # generate_block_sequence output
    block_sequence_yaml: str

    # hitl_review output
    pending_review: bool
    existing_files: dict  # filename → existing content (for diff display)

    # Set by Streamlit UI before commit_to_disk
    user_edits: dict  # filename → user-edited content

    # commit_to_disk output
    committed: bool

    # Error from any node
    error: str


class ScaffoldState(TypedDict, total=False):
    """LangGraph state for the block scaffold agent."""

    # Inputs
    domain_name: str
    extraction_description: str

    # generate_scaffold / fix_scaffold outputs
    scaffold_source: str

    # validate_syntax outputs
    syntax_valid: bool
    syntax_error: str
    retry_count: int

    # hitl_review output
    pending_review: bool

    # Set by Streamlit UI before save_to_custom_blocks
    user_source: str

    # save_to_custom_blocks output
    committed: bool

    # Error from any node
    error: str


class ValidationIssue(TypedDict):
    """A single validation finding from the deterministic validator."""

    level: str    # "error" | "warning"
    check: str    # short identifier
    message: str  # human-readable description


# ---------------------------------------------------------------------------
# Deterministic validator (pure function — no LLM, no I/O)
# ---------------------------------------------------------------------------


def validate_enrichment_rules_yaml(
    enrichment_yaml_dict: dict,
    csv_headers: list[str],
    block_sequence_dict: Optional[dict] = None,
    domain_dir: Optional[Path] = None,
) -> list[ValidationIssue]:
    """Run deterministic checks on domain pack artifacts.

    Always checks (enrichment_rules context):
      - Check 4: enrichment field name matches a CSV header → warning
      - Check 5: enrichment field matches a custom block name in sequence → warning

    Additional checks when block_sequence_dict provided:
      - Check 1: __generated__ sentinel absent → error
      - Check 2: dq_score_pre not first or dq_score_post not last → warning
      - Check 3: custom block in sequence has no matching .py file → error (needs domain_dir)
    """
    issues: list[ValidationIssue] = []

    # Extract enrichment field names from enrichment_rules
    enrich_fields: list[str] = []
    if isinstance(enrichment_yaml_dict, dict):
        for field in enrichment_yaml_dict.get("fields", []):
            if isinstance(field, dict) and "name" in field:
                enrich_fields.append(field["name"])

    # Check 4: enrichment field name matches CSV header
    header_set = {h.lower() for h in csv_headers}
    for fname in enrich_fields:
        if fname.lower() in header_set:
            issues.append(ValidationIssue(
                level="warning",
                check="enrichment_field_matches_csv_header",
                message=(
                    f"Enrichment field '{fname}' matches a CSV header — "
                    "this may re-extract an already-structured column. "
                    "Consider using RENAME in prompt_examples instead."
                ),
            ))

    if block_sequence_dict is not None:
        sequence = block_sequence_dict.get("sequence", [])
        if not isinstance(sequence, list):
            sequence = []

        # Check 1: __generated__ sentinel
        if "__generated__" not in sequence:
            issues.append(ValidationIssue(
                level="error",
                check="missing_generated_sentinel",
                message=(
                    "block_sequence.yaml is missing the '__generated__' sentinel. "
                    "This sentinel is required — it is replaced at runtime with the "
                    "DynamicMappingBlock for schema transformation."
                ),
            ))

        # Check 2: dq_score_pre first, dq_score_post last
        if sequence and sequence[0] != "dq_score_pre":
            issues.append(ValidationIssue(
                level="warning",
                check="dq_score_pre_not_first",
                message=(
                    f"First block is '{sequence[0]}' but should be 'dq_score_pre'. "
                    "Pre-enrichment DQ scoring must run before any transforms."
                ),
            ))
        if sequence and sequence[-1] != "dq_score_post":
            issues.append(ValidationIssue(
                level="warning",
                check="dq_score_post_not_last",
                message=(
                    f"Last block is '{sequence[-1]}' but should be 'dq_score_post'. "
                    "Post-enrichment DQ scoring must run after all transforms."
                ),
            ))

        # Identify custom block names in sequence (domain__name pattern)
        custom_blocks_in_seq = [
            b for b in sequence
            if isinstance(b, str) and "__" in b and b not in (
                "__generated__",
                "fuzzy_deduplicate", "column_wise_merge", "golden_record_select",
            )
        ]

        # Check 5: enrichment field matches custom block name in sequence
        enrich_field_set = {f.lower() for f in enrich_fields}
        for block_name in custom_blocks_in_seq:
            # Extract the logical name part after domain__
            parts = block_name.split("__", 1)
            logical = parts[1] if len(parts) > 1 else block_name
            # Check if any enrichment field loosely matches
            for fname in enrich_fields:
                if fname.lower() in logical.lower() or logical.lower() in fname.lower():
                    issues.append(ValidationIssue(
                        level="warning",
                        check="double_extraction_anti_pattern",
                        message=(
                            f"Custom block '{block_name}' in sequence appears to overlap "
                            f"with enrichment field '{fname}'. "
                            "This may cause double-extraction. "
                            "If '{fname}' is handled by enrichment_rules.yaml, "
                            "remove the custom block from block_sequence."
                        ),
                    ))

        # Check 3: custom block in sequence has no matching .py file
        if domain_dir is not None:
            custom_blocks_dir = domain_dir / "custom_blocks"
            for block_name in custom_blocks_in_seq:
                parts = block_name.split("__", 1)
                logical = parts[1] if len(parts) > 1 else block_name
                expected_py = custom_blocks_dir / f"{logical}.py"
                if not expected_py.exists():
                    issues.append(ValidationIssue(
                        level="error",
                        check="missing_custom_block_file",
                        message=(
                            f"Block '{block_name}' is referenced in block_sequence.yaml "
                            f"but no file exists at '{expected_py}'. "
                            "Create the custom block or remove it from the sequence."
                        ),
                    ))

    return issues


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _csv_to_headers_and_table(csv_content: str, max_rows: int = 5) -> tuple[list[str], str]:
    reader = csv.reader(io.StringIO(csv_content))
    rows = list(reader)
    if not rows:
        return [], ""
    headers = rows[0]
    data_rows = rows[1 : max_rows + 1]
    sep = " | ".join(["---"] * len(headers))
    lines = [f"| {' | '.join(headers)} |", f"| {sep} |"]
    for row in data_rows:
        padded = (row + [""] * len(headers))[: len(headers)]
        lines.append("| " + " | ".join(str(v)[:80] for v in padded) + " |")
    return headers, "\n".join(lines)


def _call_llm_for_yaml(prompt: str) -> str:
    """Call orchestrator LLM expecting {yaml: <content>} JSON response. Returns YAML string."""
    response = call_llm_json(
        model=get_orchestrator_llm(),
        messages=[{"role": "user", "content": prompt}],
    )
    if not isinstance(response, dict):
        raise ValueError(f"LLM returned non-dict: {type(response)}")
    yaml_text = response.get("yaml", "")
    if not yaml_text:
        raise ValueError("LLM returned empty 'yaml' field")
    return yaml_text.strip()


def _call_llm_for_source(prompt: str) -> str:
    """Call orchestrator LLM expecting {source: <content>} JSON response. Returns Python source."""
    response = call_llm_json(
        model=get_orchestrator_llm(),
        messages=[{"role": "user", "content": prompt}],
    )
    if not isinstance(response, dict):
        raise ValueError(f"LLM returned non-dict: {type(response)}")
    source = response.get("source", "")
    if not source:
        raise ValueError("LLM returned empty 'source' field")
    # Strip markdown fences if the LLM wrapped the Python despite instructions
    m = _FENCE_RE.search(source)
    if m:
        source = m.group(1).strip()
    return source.strip()


def _append_audit(domain_name: str, action: str, outcome: str, detail: str) -> None:
    audit_file = DOMAIN_PACKS_DIR / domain_name / ".audit.jsonl"
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "domain": domain_name,
        "action": action,
        "outcome": outcome,
        "detail": detail,
    }
    try:
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        logger.warning("Could not write audit entry for %s: %s", domain_name, exc)


# ---------------------------------------------------------------------------
# DomainKitGraph node functions
# ---------------------------------------------------------------------------


def _analyze_csv_node(state: DomainKitState) -> DomainKitState:
    csv_content = state.get("csv_content", "")
    try:
        headers, table = _csv_to_headers_and_table(csv_content)
        return {**state, "csv_headers": headers, "csv_sample_table": table}
    except Exception as exc:
        return {**state, "error": f"analyze_csv failed: {exc}", "csv_headers": [], "csv_sample_table": ""}


def _generate_enrichment_rules_node(state: DomainKitState) -> DomainKitState:
    validation_errors = state.get("validation_errors", [])
    try:
        if validation_errors:
            prompt = build_enrichment_rules_fix_prompt(
                domain_name=state.get("domain_name", ""),
                description=state.get("description", ""),
                csv_headers=state.get("csv_headers", []),
                previous_yaml=state.get("enrichment_rules_yaml", ""),
                validation_errors=validation_errors,
            )
        else:
            prompt = build_enrichment_rules_prompt(
                domain_name=state.get("domain_name", ""),
                description=state.get("description", ""),
                csv_headers=state.get("csv_headers", []),
                sample_table=state.get("csv_sample_table", ""),
            )
        yaml_text = _call_llm_for_yaml(prompt)
        return {**state, "enrichment_rules_yaml": yaml_text}
    except Exception as exc:
        return {**state, "error": f"generate_enrichment_rules failed: {exc}"}


def _validate_enrichment_rules_node(state: DomainKitState) -> DomainKitState:
    yaml_text = state.get("enrichment_rules_yaml", "")
    csv_headers = state.get("csv_headers", [])
    retry_count = state.get("retry_count", 0)

    try:
        yaml_dict = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError as exc:
        return {
            **state,
            "validation_errors": [f"YAML parse error: {exc}"],
            "enrichment_fields": [],
            "retry_count": retry_count + 1,
        }

    issues = validate_enrichment_rules_yaml(yaml_dict, csv_headers)
    errors = [i["message"] for i in issues if i["level"] == "error"]

    # Also run basic structural checks
    structural_errors: list[str] = []
    if not isinstance(yaml_dict, dict):
        structural_errors.append("Top-level value is not a mapping")
    elif "domain" not in yaml_dict:
        structural_errors.append("Missing required 'domain' key")
    elif "fields" not in yaml_dict:
        structural_errors.append("Missing required 'fields' key")

    # Validate regex patterns
    for field in yaml_dict.get("fields", []) if isinstance(yaml_dict, dict) else []:
        if not isinstance(field, dict):
            continue
        for pat in field.get("patterns", []):
            if isinstance(pat, dict) and "regex" in pat:
                try:
                    re.compile(pat["regex"])
                except re.error as exc:
                    structural_errors.append(
                        f"Field '{field.get('name', '?')}' has invalid regex "
                        f"'{pat['regex']}': {exc}"
                    )

    all_errors = structural_errors + errors
    enrich_fields = [
        f["name"] for f in yaml_dict.get("fields", [])
        if isinstance(f, dict) and "name" in f
    ] if isinstance(yaml_dict, dict) else []

    return {
        **state,
        "validation_errors": all_errors,
        "enrichment_fields": enrich_fields,
        "retry_count": retry_count + 1 if all_errors else retry_count,
    }


def _revise_enrichment_rules_node(state: DomainKitState) -> DomainKitState:
    try:
        prompt = build_enrichment_rules_fix_prompt(
            domain_name=state.get("domain_name", ""),
            description=state.get("description", ""),
            csv_headers=state.get("csv_headers", []),
            previous_yaml=state.get("enrichment_rules_yaml", ""),
            validation_errors=state.get("validation_errors", []),
        )
        yaml_text = _call_llm_for_yaml(prompt)
        return {**state, "enrichment_rules_yaml": yaml_text}
    except Exception as exc:
        return {**state, "error": f"revise_enrichment_rules failed: {exc}"}


def _generate_prompt_examples_node(state: DomainKitState) -> DomainKitState:
    try:
        prompt = build_prompt_examples_prompt(
            domain_name=state.get("domain_name", ""),
            description=state.get("description", ""),
            csv_headers=state.get("csv_headers", []),
            enrichment_fields=state.get("enrichment_fields", []),
            sample_table=state.get("csv_sample_table", ""),
        )
        yaml_text = _call_llm_for_yaml(prompt)
        return {**state, "prompt_examples_yaml": yaml_text}
    except Exception as exc:
        return {**state, "error": f"generate_prompt_examples failed: {exc}"}


def _generate_block_sequence_node(state: DomainKitState) -> DomainKitState:
    try:
        prompt = build_block_sequence_prompt(
            domain_name=state.get("domain_name", ""),
            description=state.get("description", ""),
            enrichment_fields=state.get("enrichment_fields", []),
        )
        yaml_text = _call_llm_for_yaml(prompt)
        return {**state, "block_sequence_yaml": yaml_text}
    except Exception as exc:
        return {**state, "error": f"generate_block_sequence failed: {exc}"}


def _hitl_review_node(state: DomainKitState) -> DomainKitState:
    """No-op node: set pending_review and capture existing files for diff display."""
    domain_name = state.get("domain_name", "")
    domain_dir = DOMAIN_PACKS_DIR / domain_name
    existing_files: dict = {}
    for fname in ("enrichment_rules.yaml", "prompt_examples.yaml", "block_sequence.yaml"):
        fpath = domain_dir / fname
        if fpath.exists():
            try:
                existing_files[fname] = fpath.read_text()
            except Exception:
                pass
    return {**state, "pending_review": True, "existing_files": existing_files}


def _commit_to_disk_node(state: DomainKitState) -> DomainKitState:
    """Write domain pack files. Backs up existing files, appends audit log."""
    domain_name = state.get("domain_name", "")
    domain_dir = DOMAIN_PACKS_DIR / domain_name

    user_edits: dict = state.get("user_edits", {})
    files_to_write = {
        "enrichment_rules.yaml": user_edits.get(
            "enrichment_rules.yaml", state.get("enrichment_rules_yaml", "")
        ),
        "prompt_examples.yaml": user_edits.get(
            "prompt_examples.yaml", state.get("prompt_examples_yaml", "")
        ),
        "block_sequence.yaml": user_edits.get(
            "block_sequence.yaml", state.get("block_sequence_yaml", "")
        ),
    }

    existing_files: dict = state.get("existing_files", {})
    is_overwrite = bool(existing_files)

    try:
        domain_dir.mkdir(parents=True, exist_ok=True)

        backed_up: list[str] = []
        for fname, existing_content in existing_files.items():
            bak_path = domain_dir / f"{fname}.bak"
            try:
                bak_path.write_text(existing_content)
                backed_up.append(str(bak_path.name))
            except Exception as exc:
                logger.warning("Could not write backup for %s: %s", fname, exc)

        for fname, content in files_to_write.items():
            if content:
                (domain_dir / fname).write_text(content)

        action = "overwrite" if is_overwrite else "generate"
        detail = f"committed {list(files_to_write.keys())}"
        if backed_up:
            detail += f"; backed up: {backed_up}"
        _append_audit(domain_name, action, "success", detail)

        return {**state, "committed": True}
    except Exception as exc:
        _append_audit(domain_name, "generate", "error", str(exc))
        return {**state, "error": f"commit_to_disk failed: {exc}", "committed": False}


# ---------------------------------------------------------------------------
# DomainKitGraph routing
# ---------------------------------------------------------------------------


def _route_after_validate(state: DomainKitState) -> str:
    errors = state.get("validation_errors", [])
    retry_count = state.get("retry_count", 0)
    if errors and retry_count < 2:
        return "revise_enrichment_rules"
    return "generate_prompt_examples"


# ---------------------------------------------------------------------------
# ScaffoldGraph node functions
# ---------------------------------------------------------------------------


def _generate_scaffold_node(state: ScaffoldState) -> ScaffoldState:
    syntax_error = state.get("syntax_error", "")
    try:
        if syntax_error and state.get("scaffold_source"):
            prompt = build_scaffold_fix_prompt(
                domain_name=state.get("domain_name", ""),
                extraction_description=state.get("extraction_description", ""),
                broken_source=state.get("scaffold_source", ""),
                syntax_error=syntax_error,
            )
        else:
            prompt = build_scaffold_generate_prompt(
                domain_name=state.get("domain_name", ""),
                extraction_description=state.get("extraction_description", ""),
            )
        source = _call_llm_for_source(prompt)
        return {**state, "scaffold_source": source}
    except Exception as exc:
        return {**state, "error": f"generate_scaffold failed: {exc}"}


def _validate_syntax_node(state: ScaffoldState) -> ScaffoldState:
    source = state.get("scaffold_source", "")
    retry_count = state.get("retry_count", 0)
    try:
        ast.parse(source)
        return {**state, "syntax_valid": True, "syntax_error": ""}
    except SyntaxError as exc:
        return {
            **state,
            "syntax_valid": False,
            "syntax_error": str(exc),
            "retry_count": retry_count + 1,
        }


def _fix_scaffold_node(state: ScaffoldState) -> ScaffoldState:
    try:
        prompt = build_scaffold_fix_prompt(
            domain_name=state.get("domain_name", ""),
            extraction_description=state.get("extraction_description", ""),
            broken_source=state.get("scaffold_source", ""),
            syntax_error=state.get("syntax_error", ""),
        )
        source = _call_llm_for_source(prompt)
        return {**state, "scaffold_source": source}
    except Exception as exc:
        return {**state, "error": f"fix_scaffold failed: {exc}"}


def _scaffold_hitl_review_node(state: ScaffoldState) -> ScaffoldState:
    return {**state, "pending_review": True}


def _save_to_custom_blocks_node(state: ScaffoldState) -> ScaffoldState:
    domain_name = state.get("domain_name", "")
    source = state.get("user_source") or state.get("scaffold_source", "")
    if not source:
        return {**state, "error": "No source to save", "committed": False}

    custom_blocks_dir = DOMAIN_PACKS_DIR / domain_name / "custom_blocks"
    try:
        custom_blocks_dir.mkdir(parents=True, exist_ok=True)
        # Derive filename from class name in source
        class_match = re.search(r"^class\s+(\w+)", source, re.MULTILINE)
        class_name = class_match.group(1) if class_match else f"{domain_name}_block"
        # Convert PascalCase to snake_case for filename
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()
        filename = f"{snake}.py"
        (custom_blocks_dir / filename).write_text(source)
        _append_audit(domain_name, "scaffold", "success", f"saved {filename}")
        return {**state, "committed": True}
    except Exception as exc:
        _append_audit(domain_name, "scaffold", "error", str(exc))
        return {**state, "error": f"save_to_custom_blocks failed: {exc}", "committed": False}


# ---------------------------------------------------------------------------
# ScaffoldGraph routing
# ---------------------------------------------------------------------------


def _route_after_validate_syntax(state: ScaffoldState) -> str:
    if not state.get("syntax_valid", False) and state.get("retry_count", 0) < 2:
        return "fix_scaffold"
    return "hitl_review"


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------

_KIT_NODE_MAP = {
    "analyze_csv": _analyze_csv_node,
    "generate_enrichment_rules": _generate_enrichment_rules_node,
    "validate_enrichment_rules": _validate_enrichment_rules_node,
    "revise_enrichment_rules": _revise_enrichment_rules_node,
    "generate_prompt_examples": _generate_prompt_examples_node,
    "generate_block_sequence": _generate_block_sequence_node,
    "hitl_review": _hitl_review_node,
    "commit_to_disk": _commit_to_disk_node,
}

_SCAFFOLD_NODE_MAP = {
    "generate_scaffold": _generate_scaffold_node,
    "validate_syntax": _validate_syntax_node,
    "fix_scaffold": _fix_scaffold_node,
    "hitl_review": _scaffold_hitl_review_node,
    "save_to_custom_blocks": _save_to_custom_blocks_node,
}


def build_kit_graph() -> StateGraph:
    graph: StateGraph = StateGraph(DomainKitState)

    for name, fn in _KIT_NODE_MAP.items():
        graph.add_node(name, fn)

    graph.set_entry_point("analyze_csv")
    graph.add_edge("analyze_csv", "generate_enrichment_rules")
    graph.add_edge("generate_enrichment_rules", "validate_enrichment_rules")
    graph.add_conditional_edges(
        "validate_enrichment_rules",
        _route_after_validate,
        {
            "revise_enrichment_rules": "revise_enrichment_rules",
            "generate_prompt_examples": "generate_prompt_examples",
        },
    )
    graph.add_edge("revise_enrichment_rules", "validate_enrichment_rules")
    graph.add_edge("generate_prompt_examples", "generate_block_sequence")
    graph.add_edge("generate_block_sequence", "hitl_review")
    graph.add_edge("hitl_review", END)

    return graph


def build_scaffold_graph() -> StateGraph:
    graph: StateGraph = StateGraph(ScaffoldState)

    for name, fn in _SCAFFOLD_NODE_MAP.items():
        graph.add_node(name, fn)

    graph.set_entry_point("generate_scaffold")
    graph.add_edge("generate_scaffold", "validate_syntax")
    graph.add_conditional_edges(
        "validate_syntax",
        _route_after_validate_syntax,
        {
            "fix_scaffold": "fix_scaffold",
            "hitl_review": "hitl_review",
        },
    )
    graph.add_edge("fix_scaffold", "validate_syntax")
    graph.add_edge("hitl_review", END)

    return graph


# ---------------------------------------------------------------------------
# Step runners (mirror run_step() from src/agents/graph.py)
# ---------------------------------------------------------------------------


def run_kit_step(step_name: str, state: DomainKitState) -> DomainKitState:
    """Run a single DomainKitGraph node by name. Used by Streamlit for HITL step-by-step."""
    if step_name not in _KIT_NODE_MAP:
        raise KeyError(
            f"Unknown kit step: {step_name!r}. Available: {list(_KIT_NODE_MAP.keys())}"
        )
    node_fn = _KIT_NODE_MAP[step_name]
    updates = node_fn(state)
    return updates


def run_scaffold_step(step_name: str, state: ScaffoldState) -> ScaffoldState:
    """Run a single ScaffoldGraph node by name. Used by Streamlit for HITL step-by-step."""
    if step_name not in _SCAFFOLD_NODE_MAP:
        raise KeyError(
            f"Unknown scaffold step: {step_name!r}. Available: {list(_SCAFFOLD_NODE_MAP.keys())}"
        )
    node_fn = _SCAFFOLD_NODE_MAP[step_name]
    updates = node_fn(state)
    return updates
