"""Pipeline runner — executes blocks in sequence with audit logging."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.registry.block_registry import BlockRegistry
from src.utils.csv_stream import CsvStreamReader, DEFAULT_CHUNK_SIZE

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Executes a sequence of transformation blocks on a DataFrame.

    The block_sequence may include:
    - "__generated__" sentinel: replaced by dynamically generated blocks (loaded from BlockRegistry)
    - Stage names: expanded to constituent blocks (e.g., "dedup_stage" -> 3 blocks)
    """

    def __init__(self, block_registry: BlockRegistry):
        self.block_registry = block_registry

    def run(
        self,
        df: pd.DataFrame,
        block_sequence: list[str],
        column_mapping: dict[str, str] | None = None,
        config: dict | None = None,
    ) -> tuple[pd.DataFrame, list[dict]]:
        """
        Execute blocks in sequence.

        Args:
            df: Input DataFrame
            block_sequence: Ordered list of block names. May include:
                - "__generated__": dynamically generated schema transformation blocks
                - Stage names (dedup_stage, enrich_stage): expanded to constituent blocks
            column_mapping: source_col -> unified_col rename mapping.
            config: Block configuration (DQ weights, domain, etc.)

        Returns:
            (result_df, audit_log)
        """
        config = config or {}
        audit_log = []

        if column_mapping:
            df = df.rename(columns=column_mapping)
            dupe_mask = df.columns.duplicated(keep=False)
            if dupe_mask.any():
                logger.warning(f"Duplicate columns after rename {df.columns[dupe_mask].tolist()} — keeping last occurrence")
                df = df.loc[:, ~df.columns.duplicated(keep="last")]
            audit_log.append(
                {
                    "block": "column_mapping",
                    "rows_in": len(df),
                    "rows_out": len(df),
                    "columns_renamed": column_mapping,
                }
            )

        domain = config.get("domain")
        expanded_sequence = self._expand_sequence(block_sequence, domain=domain)
        logger.info(f"Expanded sequence: {expanded_sequence}")

        coverage_warnings = self._validate_schema_coverage(
            expanded_sequence, column_mapping or {}, config,
        )
        if coverage_warnings:
            audit_log.append({
                "block": "_schema_coverage_check",
                "warnings": coverage_warnings,
                "rows_in": len(df),
                "rows_out": len(df),
            })

        for block_name in expanded_sequence:
            rows_before = len(df)

            try:
                block = self.block_registry.get(block_name)
                df = block.run(df, config)
                audit_log.append(block.audit_entry(rows_before, len(df)))
                logger.info(f"Block '{block_name}': {rows_before} -> {len(df)} rows")
            except KeyError:
                raise RuntimeError(
                    f"Block '{block_name}' not found in registry. "
                    "Agent 2 should have generated this block — pipeline cannot continue."
                )

        return df, audit_log

    def _validate_schema_coverage(
        self,
        expanded_sequence: list[str],
        column_mapping: dict[str, str],
        config: dict,
    ) -> list[str]:
        """Check that required output columns have at least one producing block."""
        unified_schema = config.get("unified_schema")
        if not unified_schema:
            return []

        required_cols = {
            col
            for col, spec in unified_schema.get("columns", {}).items()
            if spec.get("required") and not spec.get("computed")
        }

        covered_cols = set(column_mapping.values()) if column_mapping else set()
        for block_name in expanded_sequence:
            try:
                block = self.block_registry.get(block_name)
                covered_cols.update(block.outputs)
            except KeyError:
                pass

        uncovered = required_cols - covered_cols
        warnings = []
        for col in sorted(uncovered):
            msg = f"Pre-execution: required column '{col}' has no block declaring it as output"
            logger.warning(msg)
            warnings.append(msg)
        return warnings

    def _expand_sequence(self, sequence: list[str], domain: str | None = None) -> list[str]:
        """Expand stages and __generated__ sentinel in the sequence."""
        expanded = []

        for item in sequence:
            if item == "__generated__":
                generated_blocks = [
                    name
                    for name, block in self.block_registry.blocks.items()
                    if name.startswith("DYNAMIC_MAPPING_")
                    and (
                        domain is None
                        or getattr(block, "domain", "all") in ("all", domain)
                    )
                ]
                expanded.extend(generated_blocks)
            elif self.block_registry.is_stage(item):
                expanded.extend(self.block_registry.expand_stage(item))
            else:
                expanded.append(item)

        return expanded

    def run_chunked(
        self,
        source_path: str,
        block_sequence: list[str],
        column_mapping: dict[str, str] | None = None,
        config: dict | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        sep: str = ",",
    ) -> tuple[pd.DataFrame, list[dict]]:
        """
        Execute pipeline in chunks for large files.

        Args:
            source_path: Path to source CSV file
            block_sequence: Ordered list of block names
            column_mapping: source_col -> unified_col rename mapping
            config: Block configuration
            chunk_size: Rows per chunk
            sep: CSV delimiter (auto-detected by load_source_node)

        Returns:
            (result_df, audit_log)
        """
        config = config or {}
        all_audit_logs: list[dict] = []
        chunk_paths: list[Path] = []

        run_id = config.get("run_id", "run")
        output_dir = Path(config.get("output_dir", "output"))
        chunk_dir = output_dir / ".chunks"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        reader = CsvStreamReader(source_path, chunk_size=chunk_size, delimiter=sep)

        for i, chunk_df in enumerate(reader):
            logger.info(f"Processing chunk {i + 1} ({len(chunk_df)} rows)")
            chunk_result, chunk_log = self.run(
                chunk_df,
                block_sequence,
                column_mapping,
                config,
            )
            chunk_path = chunk_dir / f"{run_id}_chunk_{i:04d}.parquet"
            chunk_result.to_parquet(chunk_path, index=False)
            chunk_paths.append(chunk_path)
            all_audit_logs.append({"chunk_index": i, "logs": chunk_log})

        if not chunk_paths:
            return pd.DataFrame(), all_audit_logs

        combined_df = pd.concat(
            [pd.read_parquet(p) for p in chunk_paths], ignore_index=True
        )
        return combined_df, all_audit_logs
