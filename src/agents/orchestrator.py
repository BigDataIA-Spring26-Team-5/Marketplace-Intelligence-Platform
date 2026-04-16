"""Agent 1 — Orchestrator: schema analysis, gap detection, registry check."""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path

import pandas as pd

from src.agents.state import PipelineState
from src.agents.prompts import SCHEMA_ANALYSIS_PROMPT
from src.models.llm import call_llm_json, get_orchestrator_llm
from src.schema.analyzer import (
    profile_dataframe,
    load_unified_schema,
)
from src.registry.block_registry import BlockRegistry
from src.blocks.mapping_io import write_mapping_yaml, merge_hitl_decisions
from src.blocks.dynamic_mapping import DynamicMappingBlock

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_BLOCK_COLUMN_PROVIDERS: dict[str, str] = {
    "allergens": "extract_allergens",
    "primary_category": "llm_enrich",
    "dietary_tags": "llm_enrich",
    "is_organic": "llm_enrich",
}

# Primitives that map 1-source → N-target (no single 'target' key)
_SPLIT_PRIMITIVES = {"SPLIT"}
# Primitives that map N-source → 1-target
_UNIFY_PRIMITIVES = {"UNIFY"}
# Primitive that drops the source col entirely
_DELETE_PRIMITIVES = {"DELETE"}


def _detect_enrichment_columns(unified_schema: dict, source_schema: dict) -> list[str]:
    """Return names of enrichment columns in the unified schema absent from source data."""
    source_cols = {k for k in source_schema.keys() if k != "__meta__"}
    return [
        name
        for name, spec in unified_schema.get("columns", {}).items()
        if spec.get("enrichment") and name not in source_cols
    ]


def load_source_node(state: PipelineState) -> dict:
    """Load CSV and compute schema profile."""
    if state.get("source_df") is not None:
        return {}
    source_path = state["source_path"]
    logger.info(f"Loading source: {source_path}")

    df = pd.read_csv(source_path, na_values=["na", "Na", "n.a.", "not available", "not applicable", "-"], keep_default_na=True)
    schema = profile_dataframe(df)

    return {
        "source_df": df,
        "source_schema": schema,
    }


def _parse_llm_response(result: dict) -> tuple[dict, list, list, list]:
    """Parse LLM schema analysis response.

    Supports:
    - New format: column_mapping + operations[] + unresolvable[]
    - Legacy format: column_mapping + derivable_gaps + missing_columns

    Returns:
        (column_mapping, operations, unresolvable, legacy_gaps)
        - operations: new-style list with 'primitive' field
        - unresolvable: list of {target_column, reason, fallback}
        - legacy_gaps: non-empty only when parsing old format (for backward compat)
    """
    column_mapping = result.get("column_mapping", {})

    # New format — operations[] list
    if "operations" in result:
        operations = result.get("operations", [])
        unresolvable = result.get("unresolvable", [])
        return column_mapping, operations, unresolvable, []

    # Legacy format — derivable_gaps + missing_columns
    if "derivable_gaps" in result or "missing_columns" in result:
        derivable_gaps = result.get("derivable_gaps", [])
        missing_columns = result.get("missing_columns", [])
        # Convert to legacy_gaps for the old check_registry path
        gaps = list(derivable_gaps)
        for mc in missing_columns:
            gaps.append({
                "target_column": mc["target_column"],
                "target_type": mc.get("target_type", "string"),
                "source_column": None,
                "source_type": None,
                "action": "MISSING",
                "sample_values": [],
            })
        return column_mapping, [], [], gaps

    # Oldest fallback: flat "gaps" list
    gaps = result.get("gaps", [])
    return column_mapping, [], [], gaps


def analyze_schema_node(state: PipelineState) -> dict:
    """
    Agent 1 LLM call: analyze source schema against the gold-standard unified schema.

    Classifies each unified column using the 8-primitive taxonomy:
    RENAME, CAST, FORMAT, DELETE, ADD, SPLIT, UNIFY, DERIVE

    Raises FileNotFoundError if config/unified_schema.json is absent.
    """
    if state.get("unified_schema") is not None:
        return {}
    source_schema = state["source_schema"]
    domain = state.get("domain", "nutrition")
    model = get_orchestrator_llm()

    unified = load_unified_schema()

    if unified is None:
        raise FileNotFoundError(
            "config/unified_schema.json not found. "
            "The unified schema is the gold-standard target format and must be defined before running the pipeline."
        )

    logger.info("Unified schema found — diffing against source")

    mappable_cols = {
        name: spec
        for name, spec in unified["columns"].items()
        if not spec.get("computed")
    }
    unified_for_prompt = {"columns": mappable_cols}

    # Separate __meta__ from per-column profile before sending to LLM
    meta_block = source_schema.get("__meta__", {})
    columns_only = {k: v for k, v in source_schema.items() if k != "__meta__"}

    result = call_llm_json(
        model=model,
        messages=[
            {
                "role": "user",
                "content": SCHEMA_ANALYSIS_PROMPT.format(
                    source_schema=json.dumps(columns_only, indent=2),
                    source_meta=json.dumps(meta_block, indent=2),
                    unified_schema=json.dumps(unified_for_prompt, indent=2),
                ),
            }
        ],
    )

    column_mapping, operations, unresolvable, legacy_gaps = _parse_llm_response(result)

    # ── Derive backward-compat derivable_gaps / missing_columns from operations ──
    derivable_gaps = []
    missing_columns = []

    enrich_alias_ops: list[dict] = []

    if operations:
        # New format path
        for op in operations:
            primitive = op.get("primitive", "")
            target_col = op.get("target_column") or ""
            target_type = op.get("target_type", "string")

            if primitive == "ENRICH_ALIAS":
                enrich_alias_ops.append({
                    "target": target_col,
                    "source": op.get("source_enrichment", ""),
                })
                continue

            if primitive == "ADD":
                missing_columns.append({
                    "target_column": target_col,
                    "target_type": target_type,
                    "reason": op.get("reason", "No source data available"),
                    "_op": op,  # carry full op for registry processing
                })
            elif primitive in ("CAST", "FORMAT", "DERIVE", "SPLIT", "UNIFY"):
                derivable_gaps.append({
                    "target_column": target_col,
                    "target_type": target_type,
                    "source_column": op.get("source_column") or op.get("sources"),
                    "source_type": op.get("source_type", "string"),
                    "action": primitive,
                    "sample_values": op.get("sample_values", []),
                    "_op": op,  # carry full op
                })
            elif primitive == "DELETE":
                # DELETE ops tracked separately — they produce a drop_column YAML op
                derivable_gaps.append({
                    "target_column": op.get("source_column", ""),
                    "target_type": "string",
                    "source_column": op.get("source_column"),
                    "action": "DELETE",
                    "_op": op,
                })
        # Map unresolvable → missing_columns
        for ur in unresolvable:
            target_col = ur.get("target_column", "")
            missing_columns.append({
                "target_column": target_col,
                "target_type": "string",
                "reason": ur.get("reason", "Unresolvable — no source data"),
                "_unresolvable": True,
            })
    else:
        # Legacy format: split flat gaps list into derivable vs missing
        for gap in legacy_gaps:
            if gap.get("source_column") is None or gap.get("action") == "MISSING":
                missing_columns.append({
                    "target_column": gap.get("target_column", ""),
                    "target_type": gap.get("target_type", "string"),
                    "reason": gap.get("reason", "No source data available"),
                })
            else:
                derivable_gaps.append(gap)

    logger.info(
        f"Schema analysis: {len(column_mapping)} mappings, "
        f"{len(derivable_gaps)} derivable gaps, "
        f"{len(missing_columns)} missing columns"
    )
    if unresolvable:
        provider_cols = set(_BLOCK_COLUMN_PROVIDERS.keys())
        alias_targets = {a["target"] for a in enrich_alias_ops}
        truly_unresolvable = [
            u for u in unresolvable
            if u["target_column"] not in provider_cols and u["target_column"] not in alias_targets
        ]
        if truly_unresolvable:
            logger.info(
                f"Agent 1 unresolved columns (preliminary — Agent 1.5 may correct): "
                f"{[u['target_column'] for u in truly_unresolvable]}"
            )
        intercepted = [u["target_column"] for u in unresolvable if u["target_column"] in provider_cols]
        if intercepted:
            logger.info(
                f"Unresolvable gaps intercepted by enrichment blocks (NOT set_null): {intercepted}"
            )

    # Backward-compat gaps list (union)
    gaps = list(derivable_gaps)
    for mc in missing_columns:
        gaps.append({
            "target_column": mc["target_column"],
            "target_type": mc.get("target_type", "string"),
            "source_column": None,
            "source_type": None,
            "action": "MISSING",
            "sample_values": [],
        })

    required_mappable = {
        name
        for name, spec in unified["columns"].items()
        if spec.get("required")
        and not spec.get("computed")
        and not spec.get("enrichment")
    }
    # Covered = mapped + all target columns from operations
    op_targets: set[str] = set()
    for op in operations:
        if op.get("primitive") == "SPLIT":
            op_targets.update((op.get("target_columns") or {}).keys())
        elif op.get("target_column"):
            op_targets.add(op["target_column"])
    for mc in missing_columns:
        op_targets.add(mc["target_column"])

    covered = set(column_mapping.values()) | op_targets
    mapping_warnings = [
        f"Required unified column '{col}' not covered by mapping or gaps"
        for col in sorted(required_mappable - covered)
    ]
    for w in mapping_warnings:
        logger.warning(w)

    enrichment_to_generate = _detect_enrichment_columns(unified, source_schema)
    if enrichment_to_generate:
        logger.info(
            f"Enrichment columns absent from source (will be generated by blocks): "
            f"{enrichment_to_generate}"
        )

    if enrich_alias_ops:
        logger.info(
            f"Enrichment aliases: {[(a['target'], '←', a['source']) for a in enrich_alias_ops]}"
        )

    if missing_columns:
        provider_cols = set(_BLOCK_COLUMN_PROVIDERS.keys())
        alias_targets = {a["target"] for a in enrich_alias_ops}
        truly_missing = [
            mc for mc in missing_columns
            if mc["target_column"] not in provider_cols
            and mc["target_column"] not in alias_targets
        ]
        if truly_missing:
            logger.info(
                f"Agent 1 unresolved columns (preliminary — Agent 1.5 may correct): "
                f"{[mc['target_column'] for mc in truly_missing]}"
            )

    return {
        "unified_schema": unified,
        "unified_schema_existed": True,
        "column_mapping": column_mapping,
        "gaps": gaps,
        "derivable_gaps": derivable_gaps,
        "missing_columns": missing_columns,
        "operations": operations,          # new-style full ops list
        "unresolvable_gaps": unresolvable, # audit trail
        "enrichment_columns_to_generate": enrichment_to_generate,
        "mapping_warnings": mapping_warnings,
        "enrich_alias_ops": enrich_alias_ops,
    }


def check_registry_node(state: PipelineState) -> dict:
    """
    Check BlockRegistry for existing blocks, then build a YAML mapping file
    for all schema operations.

    All 8 primitives are handled declaratively via YAML. There is no Agent 2 —
    any gap the LLM cannot express as a known YAML action falls back to set_null
    with a warning.

    Three phases:
    A. ADD / unresolvable → set_null or set_default YAML operations
    B. CAST / FORMAT / DERIVE / SPLIT / UNIFY / DELETE → YAML operations
       (check registry first for pre-built blocks)
    C. Write YAML and register DynamicMappingBlock
    """
    if "block_registry_hits" in state:
        return {}

    block_reg = BlockRegistry.instance()
    domain = state.get("domain", "nutrition")
    dataset_name = Path(state.get("source_path", "unknown")).stem
    column_mapping = state.get("column_mapping", {})
    missing_columns = state.get("missing_columns", [])
    derivable_gaps = state.get("derivable_gaps", [])
    decisions = state.get("missing_column_decisions", {})
    # Use revised_operations from Agent 1.5 if present, else fall back to Agent 1's raw operations
    revised_operations = state.get("revised_operations")
    operations = revised_operations or state.get("operations", [])

    block_hits: dict[str, str] = {}
    yaml_operations: list[dict] = []
    # Rebuilt from revised_operations if Agent 1.5 ran (authoritative); else use analyze_schema_node output
    enrich_alias_ops: list[dict] = [] if revised_operations else list(state.get("enrich_alias_ops") or [])

    # ── New path: Process revised_operations directly if Agent 1.5 ran ──
    if revised_operations:
        logger.info(f"Processing {len(revised_operations)} revised operations from Agent 1.5")

        # Collect operations by type for proper ordering:
        # 1. DROP first (remove unwanted source columns)
        # 2. Transforms/casts (modify existing columns)
        # 3. ADD last (create new columns)
        drop_ops: list[dict] = []
        transform_ops: list[dict] = []
        add_ops: list[dict] = []

        for op in revised_operations:
            primitive = op.get("primitive", "")
            target_col = op.get("target_column", "")
            source_col = op.get("source_column")

            # Skip RENAME — handled by column_mapping in PipelineRunner
            if primitive == "RENAME":
                continue

            # ENRICH_ALIAS — required col will be filled post-enrichment; no YAML needed
            if primitive == "ENRICH_ALIAS":
                alias = {"target": target_col, "source": op.get("source_enrichment", "")}
                enrich_alias_ops.append(alias)
                logger.info(f"ENRICH_ALIAS '{target_col}' ← enrichment col '{alias['source']}'")
                continue

            # Check if enrichment block handles this column
            provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
            if provider and provider in block_reg.blocks:
                logger.info(f"Block provider for '{target_col}': {provider}")
                block_hits[target_col] = provider
                continue

            # Convert to YAML operation
            yaml_op = _llm_op_to_yaml(op, column_mapping)
            if yaml_op:
                action = yaml_op.get("action", "")
                if action == "drop_column":
                    drop_ops.append(yaml_op)
                elif primitive == "ADD" or action in ("set_null", "set_default"):
                    add_ops.append(yaml_op)
                else:
                    transform_ops.append(yaml_op)
                logger.info(f"{primitive} '{target_col or source_col}' → YAML {action}")
            elif primitive == "ADD":
                # Fallback for ADD without expressible action
                add_ops.append({
                    "target": target_col,
                    "type": op.get("target_type", "string"),
                    "action": "set_null",
                    "status": "missing",
                    "reason": op.get("reason", "No source data available"),
                })
                logger.info(f"ADD '{target_col}' → YAML set_null (fallback)")

        # Merge in order: drops first, transforms, then adds
        yaml_operations = drop_ops + transform_ops + add_ops

        # Skip legacy gap processing — revised_operations is authoritative

    # ── Legacy path: Process gaps when Agent 1.5 did not run ──────────
    if not revised_operations:
        # Phase A: ADD / unresolvable → YAML set_null
        for mc in missing_columns:
            target_col = mc["target_column"]
            target_type = mc.get("target_type", "string")

            provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
            if provider and provider in block_reg.blocks:
                logger.info(f"Block provider for missing column '{target_col}': {provider}")
                block_hits[target_col] = provider
                continue

            full_op = mc.get("_op")
            if full_op:
                yaml_op = _llm_op_to_yaml(full_op, column_mapping)
                if yaml_op:
                    yaml_operations.append(yaml_op)
                    logger.info(f"ADD op for '{target_col}' → YAML {yaml_op['action']}")
                    continue

            yaml_operations.append({
                "target": target_col,
                "type": target_type,
                "action": "set_null",
                "status": "missing",
                "reason": mc.get("reason", "No source data available"),
            })
            logger.info(f"Missing column '{target_col}' → YAML set_null")

        # Phase B: Derivable gaps → registry check or YAML
        generated_block_prefixes = (
            "COLUMN_RENAME_",
            "COLUMN_DROP_",
            "FORMAT_TRANSFORM_",
            "DYNAMIC_MAPPING_",
            "DERIVE_",
        )

        for gap in derivable_gaps:
            target_col = gap.get("target_column", "")
            action = gap.get("action", "")
            full_op = gap.get("_op")

            if action == "DELETE":
                source_col = gap.get("source_column")
                if source_col:
                    yaml_operations.append({
                        "source": source_col,
                        "action": "drop_column",
                    })
                    logger.info(f"DELETE '{source_col}' → YAML drop_column")
                continue

            provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
            if provider and provider in block_reg.blocks:
                logger.info(f"Block registry hit for gap '{target_col}': {provider}")
                block_hits[target_col] = provider
                continue

            found_existing = False
            for block_name in block_reg.blocks.keys():
                if block_name.startswith(generated_block_prefixes):
                    if target_col in block_name or block_name.endswith(f"_{target_col}"):
                        logger.info(f"Generated block found for gap '{target_col}': {block_name}")
                        block_hits[target_col] = block_name
                        found_existing = True
                        break

            if found_existing:
                continue

            if full_op:
                yaml_op = _llm_op_to_yaml(full_op, column_mapping)
                if yaml_op:
                    yaml_operations.append(yaml_op)
                    logger.info(f"{action} gap '{target_col}' → YAML {yaml_op.get('action')}")
                    continue

            source_col = gap.get("source_column")
            target_type = gap.get("target_type", "string")
            source_type = gap.get("source_type") or "string"

            if action in ("CAST", "TYPE_CAST"):
                effective_source = column_mapping.get(source_col, source_col) if source_col else None
                yaml_operations.append({
                    "target": target_col,
                    "type": target_type,
                    "action": "type_cast",
                    "source": effective_source,
                    "source_type": source_type,
                })
                logger.info(f"CAST gap '{target_col}' → YAML type_cast")
            elif action in ("FORMAT", "FORMAT_TRANSFORM"):
                effective_source = column_mapping.get(source_col, source_col) if source_col else None
                yaml_operations.append({
                    "target": target_col,
                    "type": target_type,
                    "action": "format_transform",
                    "source": effective_source,
                    "transform": "to_string",
                })
                logger.info(f"FORMAT gap '{target_col}' → YAML format_transform")
            else:
                logger.warning(
                    f"Gap '{target_col}' (primitive={action}) has no expressible YAML action "
                    "— falling back to set_null."
                )
                yaml_operations.append({
                    "target": target_col,
                    "type": gap.get("target_type", "string"),
                    "action": "set_null",
                    "status": "unresolvable",
                    "reason": f"No YAML handler for primitive '{action}'",
                })

    # ── Phase C: Apply HITL decisions, patch schema, and write YAML ──
    unified_schema = copy.deepcopy(state.get("unified_schema", {}))
    aliased_cols = {a["target"] for a in enrich_alias_ops}
    excluded_columns = []
    for col_name, decision in decisions.items():
        if col_name in aliased_cols:
            # Column will be filled by enrichment alias — ignore HITL exclusion decision
            continue
        if decision.get("action") == "exclude":
            col_spec = unified_schema.get("columns", {}).get(col_name)
            if col_spec:
                col_spec["required"] = False
                excluded_columns.append(col_name)
                logger.info(f"Excluded '{col_name}' from required schema (HITL decision)")

    if yaml_operations:
        yaml_operations = merge_hitl_decisions(yaml_operations, decisions)
        yaml_path = write_mapping_yaml(domain, dataset_name, yaml_operations)

        # Register the DynamicMappingBlock
        block = DynamicMappingBlock(domain=domain, yaml_path=str(yaml_path))
        block_reg.register_block(block)
        logger.info(f"Registered DynamicMappingBlock: {block.name}")

        mapping_yaml_path = str(yaml_path)
    else:
        mapping_yaml_path = None

    # Accurate final coverage check — fires after Agent 1.5 corrections and alias resolution
    yaml_covered = {op["target"] for op in yaml_operations if "target" in op}
    block_covered = set(block_hits.keys())
    aliased_col_targets = {a["target"] for a in enrich_alias_ops}
    final_missing = [
        mc["target_column"]
        for mc in state.get("missing_columns", [])
        if mc["target_column"] not in aliased_col_targets
        and mc["target_column"] not in block_covered
        and mc["target_column"] not in yaml_covered
    ]
    if final_missing:
        logger.warning(f"Columns with no coverage (will be set_null): {final_missing}")
    else:
        logger.info("All missing columns have coverage (alias, block, or YAML)")

    result = {
        "block_registry_hits": block_hits,
        "registry_misses": [],  # Always empty — no Agent 2
        "mapping_yaml_path": mapping_yaml_path,
        "enrich_alias_ops": enrich_alias_ops,
    }

    if excluded_columns:
        result["unified_schema"] = unified_schema

    return result


# ── Helpers ───────────────────────────────────────────────────────────


def _llm_op_to_yaml(op: dict, column_mapping: dict) -> dict | None:
    """
    Convert a new-style LLM operation dict to a DynamicMappingBlock YAML op dict.

    Returns None if the operation cannot be converted.
    """
    primitive = op.get("primitive", "")
    action = op.get("action", "")
    target_col = op.get("target_column", "")
    target_type = op.get("target_type", "string")
    source_col = op.get("source_column")

    # Resolve source through column_mapping (runner renames first)
    if source_col:
        source_col = column_mapping.get(source_col, source_col)

    if primitive == "ADD":
        if action == "set_default":
            return {
                "target": target_col,
                "type": target_type,
                "action": "set_default",
                "default_value": op.get("default_value"),
            }
        return {
            "target": target_col,
            "type": target_type,
            "action": "set_null",
            "status": "missing",
            "reason": op.get("reason", "No source data available"),
        }

    if primitive == "CAST":
        if not source_col:
            return None
        return {
            "target": target_col,
            "type": target_type,
            "action": "type_cast",
            "source": source_col,
            "source_type": op.get("source_type", "string"),
        }

    if primitive == "FORMAT":
        if not source_col:
            return None
        yaml_action = action if action in (
            "parse_date", "to_lowercase", "to_uppercase", "strip_whitespace",
            "regex_replace", "regex_extract", "truncate_string", "pad_string",
            "value_map", "format_transform",
        ) else "format_transform"
        result: dict = {
            "target": target_col,
            "type": target_type,
            "action": yaml_action,
            "source": source_col,
        }
        # Pass through extra params
        for k in ("pattern", "replacement", "transform", "format", "max_length",
                   "min_length", "fill_char", "side", "group", "mapping", "default"):
            if k in op:
                result[k] = op[k]
        # Pass through normalize_before_dedup annotation
        if "normalize_before_dedup" in op:
            result["normalize_before_dedup"] = op["normalize_before_dedup"]
        return result

    if primitive == "RENAME":
        if not source_col:
            return None
        return {
            "target": target_col,
            "type": target_type,
            "action": "rename",
            "source": source_col,
        }

    if primitive == "DELETE":
        src = op.get("source_column")
        if not src:
            return None
        return {"source": src, "action": "drop_column"}

    if primitive == "SPLIT":
        if action == "json_array_extract_multi":
            target_columns = op.get("target_columns", {})
            if not source_col or not target_columns:
                return None
            return {
                "source": source_col,
                "action": "json_array_extract_multi",
                "target_columns": target_columns,
            }
        if action == "split_column":
            column_names = op.get("column_names") or list(op.get("target_columns", {}).keys())
            if not source_col or not column_names:
                return None
            return {
                "source": source_col,
                "action": "split_column",
                "column_names": column_names,
                "delimiter": op.get("delimiter", ","),
            }
        if action == "xml_extract":
            if not source_col:
                return None
            return {
                "target": target_col,
                "type": target_type,
                "action": "xml_extract",
                "source": source_col,
                "tag": op.get("tag", ""),
            }
        return None

    if primitive == "UNIFY":
        sources = op.get("sources", [])
        # Resolve each source through column_mapping
        sources = [column_mapping.get(s, s) for s in sources]
        if action == "coalesce":
            return {
                "target": target_col,
                "type": target_type,
                "action": "coalesce",
                "sources": sources,
            }
        if action == "concat_columns":
            return {
                "target": target_col,
                "type": target_type,
                "action": "concat_columns",
                "sources": sources,
                "separator": op.get("separator", " "),
                "exclude_nulls": op.get("exclude_nulls", True),
            }
        if action == "string_template":
            return {
                "target": target_col,
                "type": target_type,
                "action": "string_template",
                "template": op.get("template", ""),
            }
        return None

    if primitive == "DERIVE":
        if not source_col and not op.get("sources"):
            return None
        sources_list = op.get("sources", [source_col] if source_col else [])
        sources_list = [column_mapping.get(s, s) for s in sources_list]
        primary_source = sources_list[0] if sources_list else None

        if action == "extract_json_field":
            if not primary_source:
                return None
            result = {
                "target": target_col,
                "type": target_type,
                "action": "extract_json_field",
                "source": primary_source,
                "key": op.get("key", ""),
            }
            if "filter" in op:
                result["filter"] = op["filter"]
            return result

        if action == "conditional_map":
            if not primary_source:
                return None
            return {
                "target": target_col,
                "type": target_type,
                "action": "conditional_map",
                "source": primary_source,
                "mapping": op.get("mapping", {}),
                "default": op.get("default"),
            }

        if action == "expression":
            return {
                "target": target_col,
                "type": target_type,
                "action": "expression",
                "expression": op.get("expression", ""),
            }

        if action == "contains_flag":
            if not primary_source:
                return None
            return {
                "target": target_col,
                "type": target_type,
                "action": "contains_flag",
                "source": primary_source,
                "keywords": op.get("keywords", []),
            }

        # Unknown DERIVE action → warn + None (caller will fall back to set_null)
        logger.warning(f"Unknown DERIVE action '{action}' for '{target_col}' — cannot convert to YAML")
        return None

    return None
