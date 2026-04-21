"""Agent 1 — Orchestrator: schema analysis, gap detection, registry check."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from src.agents.state import PipelineState
from src.agents.prompts import SCHEMA_ANALYSIS_PROMPT
from src.models.llm import call_llm_json, get_orchestrator_llm
from src.schema.analyzer import (
    profile_dataframe,
    get_unified_schema,
)
from src.schema.models import UnifiedSchema
from src.schema.sampling import adaptive_sample
from src.agents.confidence import calculate_confidence
from src.registry.block_registry import BlockRegistry
from src.blocks.mapping_io import write_mapping_yaml, merge_hitl_decisions
from src.blocks.dynamic_mapping import DynamicMappingBlock

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

import os as _os
_SCHEMA_SAMPLE_ROWS = int(_os.environ.get("SCHEMA_SAMPLE_ROWS", "5000"))

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


def _to_snake(name: str) -> str:
    """camelCase/PascalCase → snake_case for pre-normalization before LLM schema analysis."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower().replace(" ", "_").replace("-", "_")


def _detect_enrichment_columns(unified_schema: UnifiedSchema, source_schema: dict) -> list[str]:
    """Return names of enrichment columns in the unified schema absent from source data."""
    source_cols = {k for k in source_schema.keys() if k != "__meta__"}
    return [name for name in unified_schema.enrichment_columns if name not in source_cols]


def load_source_node(state: PipelineState) -> dict:
    """Load source data and compute schema profile with representative sampling.

    Supports local CSV files and GCS JSONL partitions (gs:// URIs).
    For GCS: downloads first partition only for schema analysis.
    """
    if state.get("source_df") is not None:
        return {}
    source_path = state["source_path"]
    logger.info(f"Loading source: {source_path}")

    from src.pipeline.loaders.gcs_loader import is_gcs_uri, GCSSourceLoader

    if is_gcs_uri(source_path):
        loader = GCSSourceLoader(source_path)
        df = loader.load_sample(n_rows=_SCHEMA_SAMPLE_ROWS)
        if df.empty:
            raise ValueError(f"No data loaded from GCS URI: {source_path}")
        _sep = ","  # JSONL has no separator concept; set sentinel for downstream
        logger.info(f"GCS schema sample: {len(df)} rows loaded for schema analysis")
    else:
        _NULL_SENTINELS = [
            "na",
            "Na",
            "NA",
            "n/a",
            "N/A",
            "n.a.",
            "N.A.",
            "none",
            "None",
            "NONE",
            "null",
            "Null",
            "NULL",
            "nan",
            "NaN",
            "NAN",
            "-",
            "--",
            "not available",
            "not applicable",
            "unknown",
            "Unknown",
            "UNKNOWN",
        ]
        import csv as _csv
        with open(source_path, newline="", encoding="utf-8", errors="replace") as _f:
            _sample = _f.read(8192)
        try:
            _sep = _csv.Sniffer().sniff(_sample, delimiters=",\t|").delimiter
        except _csv.Error:
            _sep = "\t" if _sample.count("\t") > _sample.count(",") else ","
        df = pd.read_csv(
            source_path,
            sep=_sep,
            na_values=_NULL_SENTINELS,
            keep_default_na=True,
            nrows=_SCHEMA_SAMPLE_ROWS,
            low_memory=False,
            on_bad_lines="skip",
        )
        logger.info(f"Schema sample: {len(df)} rows loaded for schema analysis (full data streamed during pipeline)")

    # Use adaptive sampling for representative row selection
    sampled_df, sampling_strategy = adaptive_sample(df, seed=42)
    logger.info(
        f"Sampling: method={sampling_strategy.method}, sample_size={sampling_strategy.sample_size}, "
        f"fallback={sampling_strategy.fallback_triggered}"
    )

    # Profile the sampled DataFrame
    schema = profile_dataframe(sampled_df)

    # Add sampling metadata to schema
    schema["__meta__"]["sampling_strategy"] = {
        "method": sampling_strategy.method,
        "sample_size": sampling_strategy.sample_size,
        "fallback_triggered": sampling_strategy.fallback_triggered,
        "fallback_reason": sampling_strategy.fallback_reason,
    }

    return {
        "source_df": df,
        "source_sep": _sep,
        "source_schema": schema,
        "_run_start_time": time.monotonic(),
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
            gaps.append(
                {
                    "target_column": mc["target_column"],
                    "target_type": mc.get("target_type", "string"),
                    "source_column": None,
                    "source_type": None,
                    "action": "MISSING",
                    "sample_values": [],
                }
            )
        return column_mapping, [], [], gaps

    # Oldest fallback: flat "gaps" list
    gaps = result.get("gaps", [])
    return column_mapping, [], [], gaps


def _compute_schema_fingerprint(source_schema: dict, domain: str, schema_version: str) -> str:
    """SHA-256-16 of sorted source column names + domain + schema version."""
    cols = sorted(k for k in source_schema.keys() if k != "__meta__")
    raw = json.dumps({"cols": cols, "domain": domain, "schema_version": schema_version})
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


_YAML_CACHE_FIELDS = (
    "column_mapping", "operations", "revised_operations", "mapping_yaml_path",
    "block_sequence", "enrichment_columns_to_generate", "enrich_alias_ops",
    "derivable_gaps", "missing_columns", "gaps", "unresolvable_gaps",
    "mapping_warnings", "enrich_alias_ops",
)


def analyze_schema_node(state: PipelineState) -> dict:
    """
    Agent 1 LLM call: analyze source schema against the gold-standard unified schema.

    Classifies each unified column using the 8-primitive taxonomy:
    RENAME, CAST, FORMAT, DELETE, ADD, SPLIT, UNIFY, DERIVE

    Raises FileNotFoundError if config/unified_schema.json is absent.
    """
    if state.get("operations") is not None:
        return {}
    source_schema = state["source_schema"]
    domain = state.get("domain", "nutrition")
    model = get_orchestrator_llm()

    unified = get_unified_schema()  # raises FileNotFoundError if absent

    # ── YAML cache check ─────────────────────────────────────────────────────
    cache_client = state.get("cache_client")
    schema_version = str(getattr(unified, "version", "") or "")
    _fingerprint = _compute_schema_fingerprint(source_schema, domain, schema_version)
    if cache_client is not None:
        from src.cache.client import CACHE_TTL_YAML
        _cached = cache_client.get("yaml", _fingerprint)
        if _cached is not None:
            try:
                cached_state = json.loads(_cached.decode())
                # Re-materialize YAML file to disk if it has been deleted
                yaml_text = cached_state.pop("__yaml_text__", None)
                yaml_path = cached_state.get("mapping_yaml_path")
                if yaml_text and yaml_path:
                    _ypath = Path(yaml_path)
                    if not _ypath.exists():
                        _ypath.parent.mkdir(parents=True, exist_ok=True)
                        _ypath.write_text(yaml_text)
                        logger.info(f"YAML cache: re-materialized {yaml_path}")
                logger.info(f"Cache HIT: loading YAML mapping from Redis (schema fingerprint {_fingerprint})")
                cached_state["cache_yaml_hit"] = True
                cached_state["unified_schema_existed"] = True
                return cached_state
            except Exception as e:
                logger.warning(f"YAML cache hit but deserialization failed: {e} — running LLM")
    # ─────────────────────────────────────────────────────────────────────────

    logger.info("Unified schema found — diffing against source")

    unified_for_prompt = unified.for_prompt()

    # Separate __meta__ from per-column profile before sending to LLM
    meta_block = source_schema.get("__meta__", {})
    columns_only_raw = {k: v for k, v in source_schema.items() if k != "__meta__"}
    # Pre-normalize camelCase/PascalCase → snake_case so LLM sees consistent names
    _norm_map = {_to_snake(k): k for k in columns_only_raw}
    columns_only = {_to_snake(k): v for k, v in columns_only_raw.items()}

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

    # Remap normalized column names back to original source names
    column_mapping = {_norm_map.get(src, src): tgt for src, tgt in column_mapping.items()}
    for op in operations:
        if op.get("source_column") and op["source_column"] in _norm_map:
            op["source_column"] = _norm_map[op["source_column"]]
        if isinstance(op.get("sources"), list):
            op["sources"] = [_norm_map.get(s, s) for s in op["sources"]]
    for gap in legacy_gaps:
        if gap.get("source_column") and gap["source_column"] in _norm_map:
            gap["source_column"] = _norm_map[gap["source_column"]]

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
                enrich_alias_ops.append(
                    {
                        "target": target_col,
                        "source": op.get("source_enrichment", ""),
                    }
                )
                continue

            if primitive == "ADD":
                missing_columns.append(
                    {
                        "target_column": target_col,
                        "target_type": target_type,
                        "reason": op.get("reason", "No source data available"),
                        "_op": op,  # carry full op for registry processing
                    }
                )
            elif primitive in ("CAST", "FORMAT", "DERIVE", "SPLIT", "UNIFY"):
                derivable_gaps.append(
                    {
                        "target_column": target_col,
                        "target_type": target_type,
                        "source_column": op.get("source_column") or op.get("sources"),
                        "source_type": op.get("source_type", "string"),
                        "action": primitive,
                        "sample_values": op.get("sample_values", []),
                        "_op": op,  # carry full op
                    }
                )
            elif primitive == "DELETE":
                # DELETE ops tracked separately — they produce a drop_column YAML op
                derivable_gaps.append(
                    {
                        "target_column": op.get("source_column", ""),
                        "target_type": "string",
                        "source_column": op.get("source_column"),
                        "action": "DELETE",
                        "_op": op,
                    }
                )
        # Map unresolvable → missing_columns
        for ur in unresolvable:
            target_col = ur.get("target_column", "")
            missing_columns.append(
                {
                    "target_column": target_col,
                    "target_type": "string",
                    "reason": ur.get("reason", "Unresolvable — no source data"),
                    "_unresolvable": True,
                }
            )
    else:
        # Legacy format: split flat gaps list into derivable vs missing
        for gap in legacy_gaps:
            if gap.get("source_column") is None or gap.get("action") == "MISSING":
                missing_columns.append(
                    {
                        "target_column": gap.get("target_column", ""),
                        "target_type": gap.get("target_type", "string"),
                        "reason": gap.get("reason", "No source data available"),
                    }
                )
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
            u
            for u in unresolvable
            if u["target_column"] not in provider_cols
            and u["target_column"] not in alias_targets
        ]
        if truly_unresolvable:
            _critic_label = "preliminary — Agent 2 may correct" if state.get("with_critic", False) else "final — Critic disabled"
            logger.info(
                f"Agent 1 unresolved columns ({_critic_label}): "
                f"{[u['target_column'] for u in truly_unresolvable]}"
            )
        intercepted = [
            u["target_column"]
            for u in unresolvable
            if u["target_column"] in provider_cols
        ]
        if intercepted:
            logger.info(
                f"Unresolvable gaps intercepted by enrichment blocks (NOT set_null): {intercepted}"
            )

    # Backward-compat gaps list (union)
    gaps = list(derivable_gaps)
    for mc in missing_columns:
        gaps.append(
            {
                "target_column": mc["target_column"],
                "target_type": mc.get("target_type", "string"),
                "source_column": None,
                "source_type": None,
                "action": "MISSING",
                "sample_values": [],
            }
        )

    required_mappable = {
        name for name in unified.required_columns
        if not unified.columns[name].enrichment
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

    # Hard fallback: ensure schema-declared enrichment_alias columns are always in enrich_alias_ops
    # LLM-proof — if both Agent 1 and Agent 2 miss/revert ENRICH_ALIAS, schema wins.
    alias_targets_set = {a["target"] for a in enrich_alias_ops}
    for col_name, col_spec in unified.columns.items():
        schema_alias = col_spec.enrichment_alias
        if schema_alias and col_name not in alias_targets_set:
            enrich_alias_ops.append({"target": col_name, "source": schema_alias})
            alias_targets_set.add(col_name)
            logger.info(
                f"Schema fallback: added ENRICH_ALIAS '{col_name}' ← '{schema_alias}'"
            )

    if enrich_alias_ops:
        logger.info(
            f"Enrichment aliases: {[(a['target'], '←', a['source']) for a in enrich_alias_ops]}"
        )

    if missing_columns:
        provider_cols = set(_BLOCK_COLUMN_PROVIDERS.keys())
        alias_targets = {a["target"] for a in enrich_alias_ops}
        truly_missing = [
            mc
            for mc in missing_columns
            if mc["target_column"] not in provider_cols
            and mc["target_column"] not in alias_targets
        ]
        if truly_missing:
            _critic_label = "preliminary — Agent 2 may correct" if state.get("with_critic", False) else "final — Critic disabled"
            logger.info(
                f"Agent 1 unresolved columns ({_critic_label}): "
                f"{[mc['target_column'] for mc in truly_missing]}"
            )

    # Add confidence scores to each operation based on source schema data
    if "source_schema" in state and operations:
        source_schema = state["source_schema"]
        source_columns = {k: v for k, v in source_schema.items() if k != "__meta__"}

        for op in operations:
            source_col = op.get("source_column")
            if source_col and source_col in source_columns:
                col_profile = source_columns[source_col]
                null_rate = col_profile.get("null_rate", 0.0)
                unique_count = col_profile.get("unique_count", 0)
                sample_size = (
                    source_schema.get("__meta__", {})
                    .get("sampling_strategy", {})
                    .get("sample_size", len(state.get("source_df", [])))
                )
                detected_structure = col_profile.get("detected_structure", "scalar")

                conf = calculate_confidence(
                    null_rate=null_rate,
                    unique_count=unique_count,
                    sample_size=sample_size,
                    has_source_column=True,
                    type_consistency=1.0,  # Simplified
                    detected_structure=detected_structure,
                )
                op["confidence_score"] = conf.score
                op["confidence_factors"] = conf.factors
            else:
                # No source column - low confidence
                op["confidence_score"] = 0.3
                op["confidence_factors"] = ["no_source_column"]

    return {
        "unified_schema_existed": True,
        "column_mapping": column_mapping,
        "gaps": gaps,
        "derivable_gaps": derivable_gaps,
        "missing_columns": missing_columns,
        "operations": operations,  # new-style full ops list (now with confidence scores)
        "unresolvable_gaps": unresolvable,  # audit trail
        "enrichment_columns_to_generate": enrichment_to_generate,
        "mapping_warnings": mapping_warnings,
        "enrich_alias_ops": enrich_alias_ops,
        "sampling_strategy": state.get("source_schema", {})
        .get("__meta__", {})
        .get("sampling_strategy", {}),
        # Carry fingerprint so plan_sequence_node can write the full cache entry
        "_schema_fingerprint": _fingerprint,
    }


# Identity columns that must carry normalize_before_dedup=true for fuzzy dedup
_IDENTITY_COLUMNS = {"product_name", "brand_owner", "brand_name"}

# Dtype families for type-compatibility checks (Rule 4)
_STRING_DTYPES = {"object", "string", "str"}
_FLOAT_DTYPES = {"float64", "float32", "float", "Float64"}
_INT_DTYPES = {"int64", "int32", "int", "Int64"}

_DTYPE_FAMILY = {
    **{d: "string" for d in _STRING_DTYPES},
    **{d: "float" for d in _FLOAT_DTYPES},
    **{d: "integer" for d in _INT_DTYPES},
}


def _deterministic_corrections(
    operations: list[dict],
    column_mapping: dict,
    source_schema: dict,
    unified_schema: UnifiedSchema,
) -> list[dict]:
    """Apply Rules 4, 6, 7 deterministically — no LLM needed.

    Rule 4: RENAME with incompatible types → CAST.
    Rule 6: Source columns not consumed anywhere → DELETE drop_column.
    Rule 7: Operations targeting identity columns missing normalize_before_dedup → add it.
    """
    source_cols = {k: v for k, v in source_schema.items() if k != "__meta__"}

    # --- Rule 4: type mismatch on RENAME ---
    corrected = []
    for op in operations:
        if op.get("primitive") == "RENAME":
            src = op.get("source_column", "")
            tgt = op.get("target_column", "")
            src_dtype = source_cols.get(src, {}).get("dtype", "object")
            tgt_spec = unified_schema.columns.get(tgt)
            tgt_type = tgt_spec.type if tgt_spec else "string"
            src_family = _DTYPE_FAMILY.get(src_dtype, "string")
            if src_family != tgt_type:
                op = {
                    **op,
                    "primitive": "CAST",
                    "action": "type_cast",
                    "source_type": src_dtype,
                    "target_type": tgt_type,
                }
                logger.info(
                    f"Deterministic Rule 4: RENAME '{src}'→'{tgt}' reclassified as CAST "
                    f"({src_dtype} → {tgt_type})"
                )
        corrected.append(op)
    operations = corrected

    # --- Rule 6: DELETE completeness ---
    consumed_sources: set[str] = set(column_mapping.keys())
    for op in operations:
        src = op.get("source_column")
        if src:
            consumed_sources.add(src)
        for src in op.get("sources", []):
            consumed_sources.add(src)
        # SPLIT ops use target_columns dict, source_column already captured above
    existing_deletes = {
        op.get("source_column") for op in operations if op.get("primitive") == "DELETE"
    }
    for col in sorted(source_cols.keys()):
        if col not in consumed_sources and col not in existing_deletes:
            operations.append(
                {
                    "primitive": "DELETE",
                    "source_column": col,
                    "action": "drop_column",
                }
            )
            logger.info(
                f"Deterministic Rule 6: added DELETE for uncovered source col '{col}'"
            )

    # --- Rule 7: normalize_before_dedup annotation ---
    for op in operations:
        tgt = op.get("target_column", "")
        if tgt in _IDENTITY_COLUMNS and not op.get("normalize_before_dedup"):
            op["normalize_before_dedup"] = True
            logger.debug(
                f"Deterministic Rule 7: normalize_before_dedup added to '{tgt}'"
            )

    return operations


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
        # On YAML cache hit the block was never registered in this process — re-register it.
        yaml_path = state.get("mapping_yaml_path")
        if yaml_path and Path(yaml_path).exists():
            _domain = state.get("domain", "nutrition")
            _block = DynamicMappingBlock(domain=_domain, yaml_path=yaml_path)
            BlockRegistry.instance().register_block(_block)
            logger.info(f"Re-registered DynamicMappingBlock from cached YAML: {yaml_path}")
        return {}

    block_reg = BlockRegistry.instance()
    domain = state.get("domain", "nutrition")
    dataset_name = Path(state.get("source_path", "unknown")).stem
    column_mapping = state.get("column_mapping", {})
    missing_columns = state.get("missing_columns", [])
    derivable_gaps = state.get("derivable_gaps", [])
    decisions = state.get("missing_column_decisions", {})
    # Use revised_operations from Agent 2 if present, else fall back to Agent 1's raw operations
    revised_operations = state.get("revised_operations")
    operations = revised_operations or state.get("operations", [])

    # Apply deterministic corrections (Rules 4, 6, 7) regardless of which agent produced operations
    source_schema = state.get("source_schema", {})
    operations = _deterministic_corrections(
        operations, column_mapping, source_schema, get_unified_schema()
    )

    block_hits: dict[str, str] = {}
    yaml_operations: list[dict] = []
    # Rebuilt from revised_operations if Agent 2 ran (authoritative); else use analyze_schema_node output
    enrich_alias_ops: list[dict] = (
        [] if revised_operations else list(state.get("enrich_alias_ops") or [])
    )

    # ── New path: Process revised_operations directly if Agent 2 ran ──
    if revised_operations:
        logger.info(
            f"Processing {len(revised_operations)} revised operations from Agent 2"
        )

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
                alias = {
                    "target": target_col,
                    "source": op.get("source_enrichment", ""),
                }
                enrich_alias_ops.append(alias)
                logger.info(
                    f"ENRICH_ALIAS '{target_col}' ← enrichment col '{alias['source']}'"
                )
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
                add_ops.append(
                    {
                        "target": target_col,
                        "type": op.get("target_type", "string"),
                        "action": "set_null",
                        "status": "missing",
                        "reason": op.get("reason", "No source data available"),
                    }
                )
                logger.info(f"ADD '{target_col}' → YAML set_null (fallback)")

        # Merge in order: drops first, transforms, then adds
        yaml_operations = drop_ops + transform_ops + add_ops

        # Hard fallback: ensure schema-declared enrichment_alias columns are always in enrich_alias_ops
        # Catches cases where Agent 2 reverted ENRICH_ALIAS → ADD set_null
        alias_targets_set = {a["target"] for a in enrich_alias_ops}
        for col_name, col_spec in get_unified_schema().columns.items():
            schema_alias = col_spec.enrichment_alias
            if schema_alias and col_name not in alias_targets_set:
                enrich_alias_ops.append({"target": col_name, "source": schema_alias})
                alias_targets_set.add(col_name)
                logger.info(
                    f"Schema fallback: restored ENRICH_ALIAS '{col_name}' ← '{schema_alias}' "
                    f"(Agent 2 reverted it)"
                )
                # Remove any ADD set_null that Agent 2 may have emitted for this column
                yaml_operations = [
                    op
                    for op in yaml_operations
                    if not (
                        op.get("target") == col_name
                        and op.get("action") in ("set_null", "set_default")
                    )
                ]

        # Skip legacy gap processing — revised_operations is authoritative

    # ── Legacy path: Process gaps when Agent 2 did not run ──────────
    if not revised_operations:
        # Phase A: ADD / unresolvable → YAML set_null
        for mc in missing_columns:
            target_col = mc["target_column"]
            target_type = mc.get("target_type", "string")

            provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
            if provider and provider in block_reg.blocks:
                logger.info(
                    f"Block provider for missing column '{target_col}': {provider}"
                )
                block_hits[target_col] = provider
                continue

            full_op = mc.get("_op")
            if full_op:
                yaml_op = _llm_op_to_yaml(full_op, column_mapping)
                if yaml_op:
                    yaml_operations.append(yaml_op)
                    logger.info(f"ADD op for '{target_col}' → YAML {yaml_op['action']}")
                    continue

            yaml_operations.append(
                {
                    "target": target_col,
                    "type": target_type,
                    "action": "set_null",
                    "status": "missing",
                    "reason": mc.get("reason", "No source data available"),
                }
            )
            logger.info(f"Missing column '{target_col}' → YAML set_null")

        # Phase B: Derivable gaps → registry check or YAML
        generated_block_prefixes = ("DYNAMIC_MAPPING_",)

        for gap in derivable_gaps:
            target_col = gap.get("target_column", "")
            action = gap.get("action", "")
            full_op = gap.get("_op")

            if action == "DELETE":
                source_col = gap.get("source_column")
                if source_col:
                    yaml_operations.append(
                        {
                            "source": source_col,
                            "action": "drop_column",
                        }
                    )
                    logger.info(f"DELETE '{source_col}' → YAML drop_column")
                continue

            provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
            if provider and provider in block_reg.blocks:
                logger.info(f"Block registry hit for gap '{target_col}': {provider}")
                block_hits[target_col] = provider
                continue

            # Note: In YAML-based architecture, we don't search for column-specific generated blocks
            # All gaps are handled by the domain's DynamicMappingBlock via YAML operations
            pass

            if full_op:
                yaml_op = _llm_op_to_yaml(full_op, column_mapping)
                if yaml_op:
                    yaml_operations.append(yaml_op)
                    logger.info(
                        f"{action} gap '{target_col}' → YAML {yaml_op.get('action')}"
                    )
                    continue

            source_col = gap.get("source_column")
            target_type = gap.get("target_type", "string")
            source_type = gap.get("source_type") or "string"

            if action in ("CAST", "TYPE_CAST"):
                effective_source = (
                    column_mapping.get(source_col, source_col) if source_col else None
                )
                yaml_operations.append(
                    {
                        "target": target_col,
                        "type": target_type,
                        "action": "type_cast",
                        "source": effective_source,
                        "source_type": source_type,
                    }
                )
                logger.info(f"CAST gap '{target_col}' → YAML type_cast")
            elif action in ("FORMAT", "FORMAT_TRANSFORM"):
                effective_source = (
                    column_mapping.get(source_col, source_col) if source_col else None
                )
                yaml_operations.append(
                    {
                        "target": target_col,
                        "type": target_type,
                        "action": "format_transform",
                        "source": effective_source,
                        "transform": "to_string",
                    }
                )
                logger.info(f"FORMAT gap '{target_col}' → YAML format_transform")
            else:
                logger.warning(
                    f"Gap '{target_col}' (primitive={action}) has no expressible YAML action "
                    "— falling back to set_null."
                )
                yaml_operations.append(
                    {
                        "target": target_col,
                        "type": gap.get("target_type", "string"),
                        "action": "set_null",
                        "status": "unresolvable",
                        "reason": f"No YAML handler for primitive '{action}'",
                    }
                )

    # ── Phase C: Apply HITL decisions and write YAML ──
    aliased_cols = {a["target"] for a in enrich_alias_ops}
    excluded_columns = []
    for col_name, decision in decisions.items():
        if col_name in aliased_cols:
            # Column will be filled by enrichment alias — ignore HITL exclusion decision
            continue
        if decision.get("action") == "exclude":
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

    # Accurate final coverage check — fires after Agent 2 corrections and alias resolution
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

    return {
        "block_registry_hits": block_hits,
        "registry_misses": [],  # Always empty — no Agent 2
        "mapping_yaml_path": mapping_yaml_path,
        "enrich_alias_ops": enrich_alias_ops,
        "excluded_columns": excluded_columns,
    }


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
        yaml_action = (
            action
            if action
            in (
                "parse_date",
                "to_lowercase",
                "to_uppercase",
                "strip_whitespace",
                "regex_replace",
                "regex_extract",
                "truncate_string",
                "pad_string",
                "value_map",
                "format_transform",
            )
            else "format_transform"
        )
        result: dict = {
            "target": target_col,
            "type": target_type,
            "action": yaml_action,
            "source": source_col,
        }
        # Pass through extra params
        for k in (
            "pattern",
            "replacement",
            "transform",
            "format",
            "max_length",
            "min_length",
            "fill_char",
            "side",
            "group",
            "mapping",
            "default",
        ):
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
            column_names = op.get("column_names") or list(
                op.get("target_columns", {}).keys()
            )
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
        logger.warning(
            f"Unknown DERIVE action '{action}' for '{target_col}' — cannot convert to YAML"
        )
        return None

    return None
