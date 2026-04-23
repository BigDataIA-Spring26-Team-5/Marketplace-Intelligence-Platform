"""YAML I/O utilities for declarative column mapping files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_GENERATED_DIR = Path(__file__).resolve().parent / "generated"

# All actions supported by DynamicMappingBlock
VALID_ACTIONS = {
    # Scalar creation
    "set_null",
    "set_default",
    # Type ops
    "type_cast",
    "rename",
    "drop_column",
    # Format ops
    "format_transform",
    "parse_date",
    "to_lowercase",
    "to_uppercase",
    "strip_whitespace",
    "regex_replace",
    "regex_extract",
    "truncate_string",
    "pad_string",
    "value_map",
    # Split ops
    "json_array_extract_multi",
    "split_column",
    "xml_extract",
    # Unify ops
    "coalesce",
    "concat_columns",
    "string_template",
    # Derive ops
    "extract_json_field",
    "conditional_map",
    "expression",
    "contains_flag",
}

# Actions that require a 'source' field (single source column)
_REQUIRE_SOURCE = {
    "type_cast", "rename", "format_transform",
    "parse_date", "to_lowercase", "to_uppercase", "strip_whitespace",
    "regex_replace", "regex_extract", "truncate_string", "pad_string",
    "value_map", "xml_extract", "extract_json_field",
    "conditional_map", "contains_flag",
}

# Actions that require a 'sources' field (list of source columns)
_REQUIRE_SOURCES_LIST = {"coalesce", "concat_columns"}

# Actions that require 'target_columns' dict (SPLIT multi-output)
_REQUIRE_TARGET_COLUMNS = {"json_array_extract_multi"}

# Actions that have no target column (structural)
_NO_TARGET_REQUIRED = {"drop_column"}

REQUIRED_FIELDS = {"action"}  # minimal — 'target' checked per-action below


def write_mapping_yaml(
    domain: str,
    dataset_name: str,
    operations: list[dict[str, Any]],
) -> Path:
    """Write column operations to a YAML mapping file.

    Args:
        domain: Pipeline domain (e.g., "nutrition").
        dataset_name: Source dataset stem (e.g., "usda_sample_raw").
        operations: List of operation dicts, each with at least
            {action} and contextual fields depending on the action type.

    Returns:
        Path to the written YAML file.
    """
    domain_dir = _GENERATED_DIR / domain
    domain_dir.mkdir(parents=True, exist_ok=True)

    safe_name = dataset_name.replace("/", "_")
    file_path = domain_dir / f"DYNAMIC_MAPPING_{safe_name}.yaml"
    data = {"column_operations": operations}
    file_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    logger.info(f"Wrote mapping YAML: {file_path} ({len(operations)} operations)")
    return file_path


def read_mapping_yaml(yaml_path: str | Path) -> list[dict[str, Any]]:
    """Read and validate column operations from a YAML mapping file.

    Returns:
        List of validated operation dicts.

    Raises:
        ValueError: If the YAML is malformed or contains invalid operations.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Mapping YAML not found: {path}")

    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or "column_operations" not in data:
        raise ValueError(f"Invalid mapping YAML: missing 'column_operations' key in {path}")

    operations = data["column_operations"]
    if not isinstance(operations, list):
        raise ValueError(f"Invalid mapping YAML: 'column_operations' must be a list in {path}")

    for i, op in enumerate(operations):
        if "action" not in op:
            raise ValueError(f"Operation {i} in {path} missing required field: 'action'")

        action = op["action"]
        if action not in VALID_ACTIONS:
            raise ValueError(
                f"Operation {i} in {path} has invalid action '{action}'. "
                f"Valid actions: {sorted(VALID_ACTIONS)}"
            )

        # Per-action required field checks
        if action in _REQUIRE_SOURCE and "source" not in op:
            raise ValueError(
                f"Operation {i} in {path}: action '{action}' requires a 'source' field"
            )
        if action in _REQUIRE_SOURCES_LIST and "sources" not in op:
            raise ValueError(
                f"Operation {i} in {path}: action '{action}' requires a 'sources' list"
            )
        if action in _REQUIRE_TARGET_COLUMNS and "target_columns" not in op:
            raise ValueError(
                f"Operation {i} in {path}: action '{action}' requires a 'target_columns' dict"
            )
        if action not in _NO_TARGET_REQUIRED and action not in _REQUIRE_TARGET_COLUMNS:
            if "target" not in op:
                raise ValueError(
                    f"Operation {i} in {path}: action '{action}' requires a 'target' field"
                )

    return operations


def merge_hitl_decisions(
    operations: list[dict[str, Any]],
    decisions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply HITL decisions to column operations.

    Args:
        operations: List of operation dicts (typically from check_registry_node).
        decisions: Dict mapping target_column to decision dict.
            Each decision has {action: "accept_null"|"exclude"|"set_default",
            value?: <default_value>}.

    Returns:
        Updated operations list with HITL decisions applied.
        "exclude" columns still get set_null (column is created but not required).
    """
    updated = []
    for op in operations:
        target = op.get("target")
        decision = decisions.get(target) if target else None
        if decision is None:
            updated.append(op)
            continue

        if decision.get("action") == "set_default":
            op = dict(op)
            op["action"] = "set_default"
            op["default_value"] = decision["value"]
            op.pop("status", None)
            updated.append(op)
        elif decision.get("action") in ("accept_null", "exclude"):
            # Both keep the set_null op — "exclude" additionally patches
            # the unified schema (handled in check_registry_node).
            updated.append(op)

    return updated
