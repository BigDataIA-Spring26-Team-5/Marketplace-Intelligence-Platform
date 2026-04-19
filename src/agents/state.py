"""LangGraph state schema for the ETL pipeline graph."""

from __future__ import annotations

from typing import Any, Optional, Union
from typing_extensions import TypedDict


class GapItem(TypedDict):
    """A single schema gap between source and unified schema."""

    target_column: str
    target_type: str
    source_column: Optional[str]  # None if column must be derived
    source_type: Optional[str]
    action: str  # MAP, DROP, NEW, ADD, MISSING, TYPE_CAST, DERIVE, FORMAT_TRANSFORM
    sample_values: list[str]


class MissingColumn(TypedDict):
    """A unified schema column that has no source data and no derivation path."""

    target_column: str
    target_type: str
    reason: str  # LLM explanation for why no derivation path exists


class DerivedGap(TypedDict):
    """A schema gap that can be resolved by transforming existing source columns."""

    target_column: str
    target_type: str
    source_column: Optional[str]
    source_type: Optional[str]
    action: str  # TYPE_CAST, DERIVE, FORMAT_TRANSFORM
    sample_values: list[str]


class PipelineState(TypedDict, total=False):
    """
    Full state flowing through the LangGraph pipeline.

    total=False allows nodes to set fields incrementally —
    not every field is present at every node.
    """

    # Input
    source_path: str
    source_df: Any  # pd.DataFrame — sample only (schema analysis + UI preview)
    source_schema: dict  # column_name -> {dtype, null_rate, sample_values}
    source_sep: str  # CSV delimiter auto-detected during load
    domain: str  # "nutrition", "safety", "pricing"
    enable_enrichment: bool  # user toggle — False skips allergen + llm_enrich blocks
    chunk_size: int  # rows per processing chunk (default from DEFAULT_CHUNK_SIZE)

    # Schema analysis (set by orchestrator node)
    unified_schema: dict
    unified_schema_existed: bool  # True if schema was loaded, False if derived
    gaps: list[GapItem]  # backward-compat union of derivable_gaps + missing_columns
    derivable_gaps: list[DerivedGap]  # gaps resolvable by transforming source columns
    missing_columns: list[
        MissingColumn
    ]  # columns with no source data or derivation path
    column_mapping: dict  # source_col -> unified_col
    enrichment_columns_to_generate: list[str]  # enrichment cols absent from source
    mapping_warnings: list[str]  # required unified cols not covered by mapping
    missing_column_decisions: dict  # HITL decisions: {col: {action, value?}}
    mapping_yaml_path: Optional[str]  # path to generated YAML mapping file

    # Schema operations (new 8-primitive format from LLM)
    operations: list[dict]  # full operations[] list from analyze_schema_node
    unresolvable_gaps: list[dict]  # gaps LLM flagged as unresolvable (audit trail)
    enrich_alias_ops: list[
        dict
    ]  # [{target: str, source: str}] — required cols aliased to enrichment cols

    # Agent 2 critic output
    revised_operations: list[dict]  # Agent 2's corrected operations list
    critique_notes: list[dict]  # Agent 2's audit notes, one entry per correction

    # Registry results (set by registry_check node)
    block_registry_hits: dict  # target_col -> block_name
    registry_misses: list[GapItem]  # always empty — no Agent 2

    # Pipeline execution
    block_sequence: list[str]
    sequence_reasoning: str  # Agent 3's rationale for the chosen sequence
    working_df: Any  # pd.DataFrame
    dq_score_pre: float
    dq_score_post: float

    # Enrichment
    enrichment_stats: dict  # tier -> row_count

    # Quarantine
    quarantined_df: Any  # pd.DataFrame — rows that failed post-enrichment validation
    quarantine_reasons: list[dict]  # [{row_idx, missing_fields, reason}]

    # Audit
    audit_log: list[dict]
    errors: list[str]
