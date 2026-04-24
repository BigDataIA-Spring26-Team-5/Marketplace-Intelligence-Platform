"""Guardrails for all LLM calls in the agentic pipeline.

This module provides input validation, output validation, and safety checks
for each LLM call site:
  1. Agent 1 — Schema Analysis (orchestrator.py)
  2. Agent 2 — Critic (critic.py)
  3. Agent 3 — Sequence Planner (graph.py)
  4. Strategy 3 — LLM Enrichment (enrichment/llm_tier.py)
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GuardrailResult:
    """Result of a guardrail check."""

    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sanitized_output: Optional[dict] = None

    def __bool__(self) -> bool:
        return self.passed


@dataclass
class HITLFlag:
    """Human-in-the-Loop flag raised when a guardrail detects high-impact output.

    Inspired by threshold-based HITL patterns: when an LLM output exceeds a
    configured threshold, it is flagged for human review rather than auto-applied.
    """

    triggered: bool
    reason: Optional[str] = None
    threshold_name: Optional[str] = None
    actual_value: Optional[float] = None
    threshold_value: Optional[float] = None


@dataclass
class GuardrailAudit:
    """Audit metadata attached to guardrail evaluations for traceability.

    Captures what was checked, which model produced the output, and timing —
    analogous to the calculation_details pattern in financial projection models.
    """

    call_site: str
    model_version: str = "unknown"
    checks_performed: list[str] = field(default_factory=list)
    hitl_flags: list[HITLFlag] = field(default_factory=list)
    elapsed_ms: Optional[float] = None
    timestamp: Optional[str] = None

    @property
    def requires_human_review(self) -> bool:
        """True if any HITL flag was triggered."""
        return any(f.triggered for f in self.hitl_flags)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_PRIMITIVES = frozenset(
    ["RENAME", "CAST", "FORMAT", "DELETE", "ADD", "SPLIT", "UNIFY", "DERIVE", "ENRICH_ALIAS"]
)

VALID_ADD_ACTIONS = frozenset(["set_null", "set_default"])
VALID_CAST_ACTIONS = frozenset(["type_cast"])
VALID_FORMAT_ACTIONS = frozenset([
    "parse_date", "regex_replace", "regex_extract",
    "truncate_string", "pad_string", "value_map", "format_transform",
])
VALID_SPLIT_ACTIONS = frozenset(["json_array_extract_multi", "split_column", "xml_extract"])
VALID_UNIFY_ACTIONS = frozenset(["coalesce", "concat_columns", "string_template"])
VALID_DERIVE_ACTIONS = frozenset([
    "extract_json_field", "conditional_map", "expression", "contains_flag",
])

# Safety-critical columns that must never be inferred by LLM/KNN
SAFETY_COLUMNS = frozenset(["allergens", "is_organic", "dietary_tags"])

# Columns that enrichment is allowed to fill
ENRICHABLE_COLUMNS = frozenset(["primary_category"])

# Maximum allowed response size (characters) to guard against runaway output
MAX_RESPONSE_SIZE = 200_000

# Maximum number of operations to prevent unbounded output
MAX_OPERATIONS_COUNT = 500

# Maximum time budget per LLM call (seconds) — checked externally
DEFAULT_TIMEOUT = 60

# Valid enrichment categories (must match llm_tier.py CATEGORIES)
VALID_CATEGORIES = frozenset([
    "Breakfast Cereals", "Dairy", "Meat & Poultry", "Seafood", "Bakery",
    "Confectionery", "Snacks", "Beverages", "Condiments", "Frozen Foods",
    "Fruits", "Vegetables", "Pasta & Grains", "Soups", "Baby Food",
    "Supplements", "Canned Foods", "Deli", "Pet Food", "Other",
])

# Primitives that do not require a source_column (they create or alias columns)
_NO_SOURCE_PRIMITIVES = frozenset(["ADD", "ENRICH_ALIAS", "UNIFY"])

# HITL thresholds — operations exceeding these require human approval
HITL_OPERATION_COUNT_THRESHOLD = 15  # Flag if LLM proposes > N operations
HITL_UNRESOLVABLE_THRESHOLD = 5     # Flag if > N columns are unresolvable
HITL_LOW_CONFIDENCE_THRESHOLD = 0.5 # Flag if average confidence is below this

# Numerical range bounds for clamping
CONFIDENCE_SCORE_MIN = 0.0
CONFIDENCE_SCORE_MAX = 1.0
RISK_SCORE_MIN = 1
RISK_SCORE_MAX = 5
DQ_SCORE_MIN = 0.0
DQ_SCORE_MAX = 100.0


# ---------------------------------------------------------------------------
# Input Guardrails
# ---------------------------------------------------------------------------

def validate_schema_analysis_input(
    source_schema: dict,
    unified_schema: dict,
    source_meta: Optional[dict] = None,
) -> GuardrailResult:
    """Validate inputs before calling the schema analysis LLM (Agent 1).

    Checks:
    - source_schema is non-empty and has valid column entries
    - unified_schema is non-empty
    - No excessively large payloads that could blow up token limits
    """
    errors = []
    warnings = []

    if not source_schema:
        errors.append("source_schema is empty — nothing to analyze")
    if not unified_schema:
        errors.append("unified_schema is empty — no target to map to")

    # Guard against excessively large schemas that waste tokens
    source_json = json.dumps(source_schema)
    if len(source_json) > 100_000:
        warnings.append(
            f"source_schema is very large ({len(source_json)} chars) — "
            "consider sampling or summarizing columns"
        )

    # Check for __meta__ leaking into columns (should be separate)
    non_meta_cols = {k for k in source_schema if k != "__meta__"}
    if not non_meta_cols:
        errors.append("source_schema contains only __meta__ — no actual columns")

    return GuardrailResult(passed=len(errors) == 0, errors=errors, warnings=warnings)


def validate_critic_input(
    column_mapping: dict,
    operations: list[dict],
    source_profile: dict,
    unified_schema: dict,
) -> GuardrailResult:
    """Validate inputs before calling the critic LLM (Agent 2).

    Checks:
    - At least one of column_mapping or operations is non-empty
    - source_profile has actual columns
    - No circular references (same column in mapping AND operations)
    """
    errors = []
    warnings = []

    if not column_mapping and not operations:
        errors.append("Both column_mapping and operations are empty — nothing to critique")

    if not source_profile:
        errors.append("source_profile is empty — critic has no data to verify against")

    # Check for columns that appear in both mapping and operations
    mapped_targets = set(column_mapping.values()) if column_mapping else set()
    op_targets = {
        op.get("target_column")
        for op in operations
        if op.get("target_column")
    }
    overlap = mapped_targets & op_targets
    if overlap:
        warnings.append(
            f"Columns in both column_mapping and operations (potential conflict): {overlap}"
        )

    return GuardrailResult(passed=len(errors) == 0, errors=errors, warnings=warnings)


def validate_sequence_planner_input(
    blocks_metadata: list[dict],
    domain: str,
) -> GuardrailResult:
    """Validate inputs before calling the sequence planner LLM (Agent 3).

    Checks:
    - blocks_metadata is non-empty
    - domain is a non-empty string
    """
    errors = []
    warnings = []

    if not blocks_metadata:
        errors.append("blocks_metadata is empty — no blocks to sequence")
    if not domain or not domain.strip():
        errors.append("domain is empty — planner needs domain context")

    return GuardrailResult(passed=len(errors) == 0, errors=errors, warnings=warnings)


def validate_enrichment_input(
    rows: list[dict],
    batch_size: int,
) -> GuardrailResult:
    """Validate inputs before calling the enrichment LLM (S3).

    Checks:
    - rows is non-empty
    - batch_size is reasonable
    - rows have required fields for enrichment
    """
    errors = []
    warnings = []

    if not rows:
        errors.append("No rows to enrich")
    if batch_size < 1 or batch_size > 100:
        warnings.append(f"batch_size={batch_size} is outside recommended range [1, 100]")

    # Check that rows have at least product_name for meaningful enrichment
    if rows:
        sample = rows[0] if isinstance(rows[0], dict) else {}
        if "product_name" not in sample and "name" not in sample:
            warnings.append(
                "Rows lack 'product_name' — enrichment quality will be poor"
            )

    return GuardrailResult(passed=len(errors) == 0, errors=errors, warnings=warnings)


# ---------------------------------------------------------------------------
# Output Guardrails
# ---------------------------------------------------------------------------

def validate_schema_analysis_output(
    result: Any,
    source_columns: set[str],
    unified_columns: set[str],
    enrichment_columns: set[str],
    computed_columns: set[str],
) -> GuardrailResult:
    """Validate LLM output from schema analysis (Agent 1).

    Checks:
    - Response is a dict with expected top-level keys
    - column_mapping values reference valid unified columns
    - operations use valid primitives and actions
    - No mapping to enrichment or computed columns
    - No hallucinated source columns
    - Safety columns are not mapped from source data
    """
    errors = []
    warnings = []

    if not isinstance(result, dict):
        return GuardrailResult(passed=False, errors=["LLM response is not a dict"])

    column_mapping = result.get("column_mapping", {})
    operations = result.get("operations", [])
    unresolvable = result.get("unresolvable", [])

    # --- column_mapping checks ---
    if not isinstance(column_mapping, dict):
        errors.append("column_mapping is not a dict")
        column_mapping = {}

    for src, tgt in column_mapping.items():
        if src not in source_columns and src != "__meta__":
            errors.append(
                f"column_mapping references hallucinated source column: '{src}' — "
                "this column does not exist in the source data"
            )
        if tgt in enrichment_columns:
            errors.append(
                f"column_mapping maps '{src}' to enrichment column '{tgt}' — "
                "enrichment columns are filled by downstream blocks, not source mapping"
            )
        if tgt in computed_columns:
            errors.append(
                f"column_mapping maps '{src}' to computed column '{tgt}' — "
                "computed columns are generated by the pipeline"
            )

    # --- operations checks ---
    if not isinstance(operations, list):
        errors.append("operations is not a list")
        operations = []

    if len(operations) > MAX_OPERATIONS_COUNT:
        errors.append(
            f"operations has {len(operations)} entries (max {MAX_OPERATIONS_COUNT}) — "
            "likely hallucination"
        )

    # Detect duplicate operations targeting the same column with the same primitive
    seen_targets: dict[tuple[str, str], int] = {}  # (primitive, target) -> first index
    for i, op in enumerate(operations):
        if not isinstance(op, dict):
            errors.append(f"operations[{i}] is not a dict")
            continue

        primitive = op.get("primitive")
        if primitive not in VALID_PRIMITIVES:
            errors.append(f"operations[{i}]: invalid primitive '{primitive}'")
            continue

        target = op.get("target_column", "")
        dup_key = (primitive, target)
        if target and dup_key in seen_targets:
            warnings.append(
                f"operations[{i}]: duplicate ({primitive}, '{target}') — "
                f"same as operations[{seen_targets[dup_key]}] (possible hallucination loop)"
            )
        elif target:
            seen_targets[dup_key] = i

        # Hallucination check: source_column must exist in actual source data
        src_col = op.get("source_column")
        if src_col and src_col not in source_columns and primitive not in _NO_SOURCE_PRIMITIVES:
            errors.append(
                f"operations[{i}]: references hallucinated source column '{src_col}' — "
                "column does not exist in source data"
            )

        # Validate action per primitive
        action = op.get("action")
        if primitive == "ADD" and action not in VALID_ADD_ACTIONS:
            errors.append(f"operations[{i}]: ADD with invalid action '{action}'")
        elif primitive == "CAST" and action not in VALID_CAST_ACTIONS:
            errors.append(f"operations[{i}]: CAST with invalid action '{action}'")
        elif primitive == "FORMAT" and action not in VALID_FORMAT_ACTIONS:
            errors.append(f"operations[{i}]: FORMAT with invalid action '{action}'")
        elif primitive == "SPLIT" and action not in VALID_SPLIT_ACTIONS:
            errors.append(f"operations[{i}]: SPLIT with invalid action '{action}'")
        elif primitive == "UNIFY" and action not in VALID_UNIFY_ACTIONS:
            errors.append(f"operations[{i}]: UNIFY with invalid action '{action}'")
        elif primitive == "DERIVE" and action not in VALID_DERIVE_ACTIONS:
            errors.append(f"operations[{i}]: DERIVE with invalid action '{action}'")

        # Safety: no operations should target safety columns
        target = op.get("target_column")
        if target in SAFETY_COLUMNS and primitive not in ("ENRICH_ALIAS",):
            errors.append(
                f"operations[{i}]: targets safety column '{target}' with "
                f"primitive '{primitive}' — safety columns must only be filled "
                "by extraction from product text (S1 deterministic), never mapped"
            )

        # No operations on computed columns
        if target in computed_columns:
            errors.append(
                f"operations[{i}]: targets computed column '{target}' — skip these"
            )

    return GuardrailResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        sanitized_output=result,
    )


def validate_critic_output(
    result: Any,
    original_operations: list[dict],
    unified_columns: Optional[set[str]] = None,
) -> GuardrailResult:
    """Validate LLM output from the critic (Agent 2).

    Checks:
    - Response is a dict with revised_operations and critique_notes
    - revised_operations uses valid primitives
    - Critic didn't hallucinate new primitives or nonsense
    - revised_operations count is reasonable relative to input
    - Target columns exist in the unified schema (no hallucinated targets)
    """
    errors = []
    warnings = []

    if not isinstance(result, dict):
        return GuardrailResult(passed=False, errors=["Critic response is not a dict"])

    revised = result.get("revised_operations")
    notes = result.get("critique_notes", [])

    if revised is None:
        warnings.append("Critic returned no revised_operations — using original")
        return GuardrailResult(passed=True, warnings=warnings, sanitized_output=result)

    if not isinstance(revised, list):
        errors.append("revised_operations is not a list")
        return GuardrailResult(passed=False, errors=errors)

    # Check for excessive additions (hallucination signal)
    orig_count = len(original_operations)
    if len(revised) > orig_count * 3 and len(revised) > 20:
        errors.append(
            f"Critic output {len(revised)} operations vs {orig_count} input — "
            "likely hallucination, rejecting"
        )

    # Validate primitives and target columns in revised operations
    for i, op in enumerate(revised):
        if not isinstance(op, dict):
            errors.append(f"revised_operations[{i}] is not a dict")
            continue
        primitive = op.get("primitive")
        if primitive not in VALID_PRIMITIVES:
            errors.append(f"revised_operations[{i}]: invalid primitive '{primitive}'")

        # Check for hallucinated target columns not in unified schema
        if unified_columns:
            target = op.get("target_column")
            if target and target not in unified_columns and primitive != "DELETE":
                warnings.append(
                    f"revised_operations[{i}]: target_column '{target}' "
                    "not in unified schema (critic may have hallucinated a column)"
                )

    # Validate critique_notes structure
    if notes and isinstance(notes, list):
        for note in notes:
            if isinstance(note, dict) and "rule" not in note:
                warnings.append("critique_note missing 'rule' field")

    return GuardrailResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        sanitized_output=result,
    )


def validate_sequence_planner_output(
    result: Any,
    required_blocks: list[str],
) -> GuardrailResult:
    """Validate LLM output from the sequence planner (Agent 3).

    Checks:
    - Response contains block_sequence as a list
    - All required blocks are present (none dropped)
    - No unknown blocks injected
    - dq_score_pre is first, dq_score_post is last
    - Ordering constraints are respected
    """
    errors = []
    warnings = []

    if not isinstance(result, dict):
        return GuardrailResult(passed=False, errors=["Planner response is not a dict"])

    sequence = result.get("block_sequence")
    if not isinstance(sequence, list):
        errors.append("block_sequence is not a list")
        return GuardrailResult(passed=False, errors=errors)

    required_set = set(required_blocks)
    sequence_set = set(sequence)

    # Check for missing blocks
    missing = required_set - sequence_set
    if missing:
        warnings.append(f"Blocks missing from sequence (will be appended): {missing}")

    # Check for unknown blocks injected by LLM
    unknown = sequence_set - required_set
    if unknown:
        errors.append(f"Unknown blocks in sequence (hallucinated): {unknown}")

    # Ordering constraints
    if sequence:
        if sequence[0] != "dq_score_pre" and "dq_score_pre" in sequence_set:
            errors.append("dq_score_pre must be first in the sequence")
        if sequence[-1] != "dq_score_post" and "dq_score_post" in sequence_set:
            errors.append("dq_score_post must be last in the sequence")

    # Check normalization before dedup ordering
    norm_blocks = {"strip_whitespace", "lowercase_brand", "remove_noise_words", "strip_punctuation"}
    dedup_blocks = {"fuzzy_deduplicate", "column_wise_merge", "golden_record_select"}

    norm_indices = [sequence.index(b) for b in norm_blocks if b in sequence]
    dedup_indices = [sequence.index(b) for b in dedup_blocks if b in sequence]

    if norm_indices and dedup_indices:
        if max(norm_indices) > min(dedup_indices):
            errors.append(
                "Normalization blocks must run before deduplication blocks"
            )

    # Any allergen-extraction block must run before llm_enrich
    allergen_blocks = [b for b in sequence if "extract_allergens" in b]
    if allergen_blocks and "llm_enrich" in sequence:
        llm_idx = sequence.index("llm_enrich")
        for ab in allergen_blocks:
            if sequence.index(ab) > llm_idx:
                errors.append(f"{ab} must run before llm_enrich")

    return GuardrailResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        sanitized_output=result,
    )


def validate_enrichment_output(
    result: Any,
    batch_size: int,
    batch_indices: list[int],
) -> GuardrailResult:
    """Validate LLM output from LLM enrichment (S3).

    Checks:
    - Response is a dict with 'results' list
    - Each result has valid idx within batch bounds
    - primary_category values are non-empty strings
    - No safety column values are present (LLM must not infer these)
    """
    errors = []
    warnings = []

    if not isinstance(result, dict):
        return GuardrailResult(passed=False, errors=["Enrichment response is not a dict"])

    results = result.get("results")
    if not isinstance(results, list):
        errors.append("'results' key is missing or not a list")
        return GuardrailResult(passed=False, errors=errors)

    valid_results = []
    for i, item in enumerate(results):
        if not isinstance(item, dict):
            warnings.append(f"results[{i}] is not a dict — skipping")
            continue

        idx = item.get("idx")
        if idx is None or not isinstance(idx, int):
            warnings.append(f"results[{i}] has invalid idx: {idx}")
            continue

        if idx < 0 or idx >= len(batch_indices):
            warnings.append(f"results[{i}] idx={idx} out of bounds [0, {len(batch_indices)})")
            continue

        category = item.get("primary_category")
        if category is not None and not isinstance(category, str):
            warnings.append(f"results[{i}] primary_category is not a string")
            continue

        # Hallucination check: category must be from the allowed set
        if category is not None and category not in VALID_CATEGORIES:
            warnings.append(
                f"results[{i}] primary_category='{category}' is not in the "
                "allowed categories list (possible hallucination) — will be kept "
                "but flagged for review"
            )

        # SAFETY: LLM must NOT return safety columns
        for safety_col in SAFETY_COLUMNS:
            if safety_col in item and item[safety_col] is not None:
                errors.append(
                    f"results[{i}] contains safety column '{safety_col}' — "
                    "LLM must never infer safety-critical fields. Removing."
                )
                item.pop(safety_col)

        valid_results.append(item)

    # Replace results with sanitized version
    sanitized = {"results": valid_results}

    if not valid_results and results:
        warnings.append("All enrichment results failed validation")

    return GuardrailResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        sanitized_output=sanitized,
    )


# ---------------------------------------------------------------------------
# Response-Level Guardrails (applied to raw LLM output)
# ---------------------------------------------------------------------------

def check_response_size(raw_response: str) -> GuardrailResult:
    """Reject responses that exceed maximum size (hallucination/loop signal)."""
    if len(raw_response) > MAX_RESPONSE_SIZE:
        return GuardrailResult(
            passed=False,
            errors=[
                f"LLM response is {len(raw_response)} chars "
                f"(max {MAX_RESPONSE_SIZE}) — likely runaway generation"
            ],
        )
    return GuardrailResult(passed=True)


def check_json_parseable(raw_response: str) -> GuardrailResult:
    """Ensure the response contains parseable JSON."""
    try:
        json.loads(raw_response)
        return GuardrailResult(passed=True)
    except json.JSONDecodeError:
        # Try markdown-fenced extraction (matches call_llm_json fallback)
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', raw_response)
        if m:
            try:
                json.loads(m.group(1).strip())
                return GuardrailResult(
                    passed=True,
                    warnings=["JSON was wrapped in markdown fences"],
                )
            except json.JSONDecodeError:
                pass
        return GuardrailResult(
            passed=False,
            errors=["Response is not valid JSON and no JSON block found"],
        )


def check_no_prompt_leakage(raw_response: str) -> GuardrailResult:
    """Detect if the LLM echoed back the system/user prompt (confusion signal)."""
    leakage_markers = [
        "You are a schema analysis agent",
        "You are a senior data engineer reviewing",
        "You are a pipeline sequence planner",
        "## Incoming Source Schema",
        "## 8-Primitive Taxonomy",
        "## Verification Rules",
    ]
    warnings = []
    for marker in leakage_markers:
        if marker in raw_response:
            warnings.append(f"Response contains prompt fragment: '{marker[:40]}...'")

    if warnings:
        return GuardrailResult(passed=True, warnings=warnings)
    return GuardrailResult(passed=True)


# ---------------------------------------------------------------------------
# Composite Guardrail Runners
# ---------------------------------------------------------------------------

def run_input_guardrails(call_site: str, **kwargs) -> GuardrailResult:
    """Run input guardrails for a specific LLM call site.

    Args:
        call_site: One of 'schema_analysis', 'critic', 'sequence_planner', 'enrichment'
        **kwargs: Arguments specific to the call site's validator

    Returns:
        GuardrailResult — if not passed, the LLM call should be skipped.
    """
    validators = {
        "schema_analysis": validate_schema_analysis_input,
        "critic": validate_critic_input,
        "sequence_planner": validate_sequence_planner_input,
        "enrichment": validate_enrichment_input,
    }

    validator = validators.get(call_site)
    if not validator:
        logger.warning(f"No input guardrail defined for call_site='{call_site}'")
        return GuardrailResult(passed=True)

    result = validator(**kwargs)

    if not result.passed:
        logger.error(f"[Guardrail:input:{call_site}] BLOCKED: {result.errors}")
    for w in result.warnings:
        logger.warning(f"[Guardrail:input:{call_site}] {w}")

    return result


def run_output_guardrails(
    call_site: str,
    raw_response: str,
    parsed_result: Any,
    **kwargs,
) -> GuardrailResult:
    """Run output guardrails for a specific LLM call site.

    Applies response-level checks first, then call-site-specific validation.

    Args:
        call_site: One of 'schema_analysis', 'critic', 'sequence_planner', 'enrichment'
        raw_response: The raw string response from the LLM
        parsed_result: The JSON-parsed response
        **kwargs: Arguments specific to the call site's validator

    Returns:
        GuardrailResult — if not passed, the result should be rejected/retried.
    """
    all_errors = []
    all_warnings = []

    # Response-level checks
    size_check = check_response_size(raw_response)
    if not size_check:
        return size_check

    leakage_check = check_no_prompt_leakage(raw_response)
    all_warnings.extend(leakage_check.warnings)

    # Call-site-specific validation
    validators = {
        "schema_analysis": validate_schema_analysis_output,
        "critic": validate_critic_output,
        "sequence_planner": validate_sequence_planner_output,
        "enrichment": validate_enrichment_output,
    }

    validator = validators.get(call_site)
    if validator:
        result = validator(parsed_result, **kwargs)
        all_errors.extend(result.errors)
        all_warnings.extend(result.warnings)
        sanitized = result.sanitized_output
    else:
        sanitized = parsed_result

    final = GuardrailResult(
        passed=len(all_errors) == 0,
        errors=all_errors,
        warnings=all_warnings,
        sanitized_output=sanitized,
    )

    if not final.passed:
        logger.error(f"[Guardrail:output:{call_site}] BLOCKED: {final.errors}")
    for w in final.warnings:
        logger.warning(f"[Guardrail:output:{call_site}] {w}")

    return final


# ---------------------------------------------------------------------------
# HITL Threshold Guardrails
# ---------------------------------------------------------------------------

def check_hitl_thresholds(
    call_site: str,
    parsed_result: dict,
    confidence_scores: Optional[list[float]] = None,
) -> list[HITLFlag]:
    """Evaluate whether an LLM output should be flagged for human review.

    Applies threshold-based checks analogous to the HITL pattern in financial
    projection models: if an output metric exceeds a configured threshold, the
    result is flagged with a reason string for human approval.

    Args:
        call_site: The agent/call site being evaluated.
        parsed_result: The parsed LLM response.
        confidence_scores: Optional list of confidence scores for operations.

    Returns:
        List of HITLFlag instances (empty if nothing triggered).
    """
    flags: list[HITLFlag] = []

    if call_site == "schema_analysis":
        operations = parsed_result.get("operations", [])
        unresolvable = parsed_result.get("unresolvable", [])

        # Flag: too many operations suggest a complex/risky transformation
        if len(operations) > HITL_OPERATION_COUNT_THRESHOLD:
            flags.append(HITLFlag(
                triggered=True,
                reason=(
                    f"LLM proposed {len(operations)} operations "
                    f"(threshold: {HITL_OPERATION_COUNT_THRESHOLD}) — "
                    "high complexity requires human review"
                ),
                threshold_name="operation_count",
                actual_value=float(len(operations)),
                threshold_value=float(HITL_OPERATION_COUNT_THRESHOLD),
            ))

        # Flag: too many unresolvable columns suggest data quality issues
        if len(unresolvable) > HITL_UNRESOLVABLE_THRESHOLD:
            flags.append(HITLFlag(
                triggered=True,
                reason=(
                    f"{len(unresolvable)} columns marked unresolvable "
                    f"(threshold: {HITL_UNRESOLVABLE_THRESHOLD}) — "
                    "significant data gaps require human decision"
                ),
                threshold_name="unresolvable_count",
                actual_value=float(len(unresolvable)),
                threshold_value=float(HITL_UNRESOLVABLE_THRESHOLD),
            ))

        # Flag: low average confidence across operations
        if confidence_scores:
            avg_confidence = sum(confidence_scores) / len(confidence_scores)
            if avg_confidence < HITL_LOW_CONFIDENCE_THRESHOLD:
                flags.append(HITLFlag(
                    triggered=True,
                    reason=(
                        f"Average confidence score {avg_confidence:.2f} is below "
                        f"threshold ({HITL_LOW_CONFIDENCE_THRESHOLD}) — "
                        "low-confidence mappings require human verification"
                    ),
                    threshold_name="avg_confidence",
                    actual_value=avg_confidence,
                    threshold_value=HITL_LOW_CONFIDENCE_THRESHOLD,
                ))

    elif call_site == "enrichment":
        results = parsed_result.get("results", [])
        # Flag: if enrichment resolves a large batch, spot-check is warranted
        if len(results) > 50:
            flags.append(HITLFlag(
                triggered=True,
                reason=(
                    f"Enrichment batch resolved {len(results)} categories in one call — "
                    "large batch outputs should be spot-checked"
                ),
                threshold_name="enrichment_batch_size",
                actual_value=float(len(results)),
                threshold_value=50.0,
            ))

    return flags


# ---------------------------------------------------------------------------
# Numerical Clamping Utilities
# ---------------------------------------------------------------------------

def clamp_value(value: float, min_val: float, max_val: float) -> float:
    """Clamp a numeric value to [min_val, max_val].

    Prevents extreme or out-of-range values from propagating through the pipeline.
    Analogous to risk_multiplier clamping in financial projection guardrails.
    """
    return max(min_val, min(value, max_val))


def validate_confidence_score(score: float) -> float:
    """Validate and clamp a confidence score to [0.0, 1.0]."""
    if not isinstance(score, (int, float)):
        logger.warning(f"Confidence score is not numeric: {score}, defaulting to 0.0")
        return 0.0
    return clamp_value(float(score), CONFIDENCE_SCORE_MIN, CONFIDENCE_SCORE_MAX)


def validate_dq_score(score: float) -> float:
    """Validate and clamp a data quality score to [0.0, 100.0]."""
    if not isinstance(score, (int, float)):
        logger.warning(f"DQ score is not numeric: {score}, defaulting to 0.0")
        return 0.0
    return clamp_value(float(score), DQ_SCORE_MIN, DQ_SCORE_MAX)


def validate_risk_score(score: int) -> int:
    """Validate and clamp a risk score to [1, 5]."""
    if not isinstance(score, int):
        logger.warning(f"Risk score is not an int: {score}, defaulting to 3")
        return 3
    return int(clamp_value(float(score), float(RISK_SCORE_MIN), float(RISK_SCORE_MAX)))


# ---------------------------------------------------------------------------
# Full Guardrail Pipeline with Audit
# ---------------------------------------------------------------------------

def run_guardrails_with_audit(
    call_site: str,
    raw_response: str,
    parsed_result: Any,
    model_version: str = "unknown",
    confidence_scores: Optional[list[float]] = None,
    **kwargs,
) -> tuple[GuardrailResult, GuardrailAudit]:
    """Run the complete guardrail pipeline and return both result and audit trail.

    This is the recommended entry point for production use. It combines:
    1. Output validation (structural + semantic checks)
    2. HITL threshold evaluation
    3. Audit metadata collection

    Returns:
        Tuple of (GuardrailResult, GuardrailAudit) — the audit captures what
        was checked and whether human review is needed, regardless of pass/fail.
    """
    import datetime

    start = time.time()

    # Run standard output guardrails
    result = run_output_guardrails(
        call_site=call_site,
        raw_response=raw_response,
        parsed_result=parsed_result,
        **kwargs,
    )

    # Evaluate HITL thresholds
    hitl_flags = check_hitl_thresholds(
        call_site=call_site,
        parsed_result=parsed_result,
        confidence_scores=confidence_scores,
    )

    elapsed = (time.time() - start) * 1000

    # Build audit trail
    checks = ["response_size", "prompt_leakage", f"{call_site}_structure"]
    if confidence_scores:
        checks.append("confidence_threshold")

    audit = GuardrailAudit(
        call_site=call_site,
        model_version=model_version,
        checks_performed=checks,
        hitl_flags=hitl_flags,
        elapsed_ms=round(elapsed, 2),
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )

    # Log HITL flags
    if audit.requires_human_review:
        for flag in hitl_flags:
            if flag.triggered:
                logger.warning(f"[Guardrail:HITL:{call_site}] {flag.reason}")

    return result, audit
