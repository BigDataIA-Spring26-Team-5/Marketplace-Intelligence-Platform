"""
CLI for the ETL pipeline with checkpoint/resume support.

Usage:
    python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
    python -m src.pipeline.cli --source data/fda_recalls_sample.csv --resume
    python -m src.pipeline.cli --source data/usda_sample_raw.csv --force-fresh
    python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/*.jsonl --domain nutrition
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from pathlib import Path
from typing import Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.agents.graph import build_graph
from src.pipeline.checkpoint import CheckpointManager
from src.pipeline.loaders.gcs_loader import is_gcs_uri

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _gcs_checkpoint_source_file(uri: str) -> Path:
    """Return a synthetic Path for GCS URIs so CheckpointManager can store the source key.

    We use the URI's SHA256 as a stable filename — the actual file doesn't exist
    on disk, but CheckpointManager.create() needs a Path. We override the SHA256
    logic below via _create_gcs_checkpoint().
    """
    digest = hashlib.sha256(uri.encode()).hexdigest()[:16]
    return Path(f"gcs_{digest}.jsonl")


def _create_gcs_checkpoint(checkpoint_mgr: CheckpointManager, uri: str) -> str:
    """Create a checkpoint for a GCS source, using URI hash instead of file SHA256."""
    import uuid
    import sqlite3
    from datetime import datetime, timezone
    from src.pipeline.checkpoint.manager import _get_schema_version

    run_id = str(uuid.uuid4())
    schema_version = _get_schema_version()
    uri_sha256 = hashlib.sha256(uri.encode()).hexdigest()

    conn = checkpoint_mgr._get_connection()
    try:
        conn.execute(
            """INSERT INTO checkpoints
               (run_id, source_file, source_sha256, schema_version, created_at, resume_state)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                uri,
                uri_sha256,
                schema_version,
                datetime.now(timezone.utc).isoformat(),
                "none",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    logger.info(f"Created GCS checkpoint: run_id={run_id}, source={uri}")
    return run_id


def run_pipeline(
    source_path: str,
    domain: str,
    resume: bool = False,
    force_fresh: bool = False,
    chunk_size: int = 10000,
    with_critic: bool = False,
    pipeline_mode: str = "full",
) -> dict:
    """Execute the pipeline with checkpoint support."""
    gcs_source = is_gcs_uri(source_path)

    if not gcs_source:
        source_file = Path(source_path)
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

    checkpoint_mgr = CheckpointManager()

    if force_fresh:
        logger.info("--force-fresh: clearing all checkpoint data")
        checkpoint_mgr.force_fresh()

    run_id: Optional[str] = None

    if resume:
        checkpoint = checkpoint_mgr.get_resume_state()
        if checkpoint:
            if gcs_source:
                # For GCS: validate by matching URI (stored as source_file)
                stored_uri = checkpoint.get("source_file", "")
                if stored_uri == source_path:
                    run_id = checkpoint["run_id"]
                    logger.info(f"Resuming from checkpoint: run_id={run_id}")
                else:
                    logger.warning("Checkpoint source URI mismatch. Starting fresh.")
                    checkpoint_mgr.force_fresh()
            else:
                source_file = Path(source_path)
                is_valid, msg = checkpoint_mgr.validate_checkpoint(source_file)
                if is_valid:
                    run_id = checkpoint["run_id"]
                    logger.info(f"Resuming from checkpoint: run_id={run_id}")
                    logger.info(f"Completed chunks: {len([c for c in checkpoint.get('chunks', []) if c['status'] == 'completed'])}")
                else:
                    logger.warning(f"Checkpoint invalid: {msg}. Starting fresh.")
                    checkpoint_mgr.force_fresh()
        else:
            logger.info("No checkpoint found, starting fresh")

    if not run_id:
        if gcs_source:
            run_id = _create_gcs_checkpoint(checkpoint_mgr, source_path)
        else:
            run_id = checkpoint_mgr.create(
                source_file=Path(source_path),
                block_sequence=[],
                config={},
            )
        logger.info(f"Created new checkpoint: run_id={run_id}")

    graph = build_graph()

    # Resolve glob → first blob's parent folder for stable dataset_name and Silver path
    resolved_source_name = None
    if is_gcs_uri(source_path) and "*" in source_path:
        from src.pipeline.loaders.gcs_loader import GCSSourceLoader
        try:
            _loader = GCSSourceLoader(source_path)
            _first_blob = _loader._list_blobs()[0]
            _blob_parts = _first_blob.name.rstrip("/").split("/")
            resolved_source_name = (
                _blob_parts[-2] if len(_blob_parts) >= 2 else Path(_blob_parts[-1]).stem
            )
        except Exception as _e:
            logger.warning(f"Could not resolve glob to first blob: {_e}")

    result = graph.invoke({
        "source_path": source_path,
        "resolved_source_name": resolved_source_name,
        "domain": domain,
        "missing_column_decisions": {},
        "chunk_size": chunk_size,
        "with_critic": with_critic,
        "pipeline_mode": pipeline_mode,
    })

    try:
        from src.enrichment.corpus import INDEX_PATH, META_PATH
        checkpoint_mgr.save_checkpoint(
            run_id=run_id,
            chunk_index=0,
            chunk_data={
                "record_count": len(result.get("working_df", [])),
                "dq_score_pre": result.get("dq_score_pre", 0),
                "dq_score_post": result.get("dq_score_post", 0),
            },
            plan_yaml=yaml.dump({"block_sequence": result.get("block_sequence", [])}),
            corpus_index_path=INDEX_PATH if INDEX_PATH.exists() else None,
            corpus_metadata_path=META_PATH if META_PATH.exists() else None,
        )
        logger.info("Checkpoint saved")
    except Exception as e:
        logger.warning(f"Failed to save checkpoint: {e}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline with Checkpoint Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to source CSV file or GCS URI (gs://bucket/path/*.jsonl)",
    )
    parser.add_argument(
        "--domain",
        default="nutrition",
        choices=["nutrition", "safety", "pricing"],
        help="Domain for the pipeline (default: nutrition)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint if available",
    )
    parser.add_argument(
        "--force-fresh",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Rows per chunk for large file processing (default: 10000)",
    )
    parser.add_argument(
        "--with-critic",
        action="store_true",
        help="Enable Agent 2 (Critic) for schema correction review. Off by default.",
    )
    parser.add_argument(
        "--mode",
        default="full",
        choices=["full", "silver", "gold"],
        help=(
            "Pipeline mode: "
            "'full' (default) = schema transform + dedup + enrichment; "
            "'silver' = schema transform only, output written to GCS Silver as Parquet; "
            "'gold' = read Silver Parquet, run dedup + enrichment, write to BigQuery."
        ),
    )

    args = parser.parse_args()

    result = run_pipeline(
        source_path=args.source,
        domain=args.domain,
        resume=args.resume,
        force_fresh=args.force_fresh,
        chunk_size=args.chunk_size,
        with_critic=args.with_critic,
        pipeline_mode=args.mode,
    )

    rows = len(result.get("working_df", []))
    dq_pre = result.get("dq_score_pre", 0)
    dq_post = result.get("dq_score_post", 0)

    logger.info(f"Pipeline complete: {rows} rows, DQ: {dq_pre}% -> {dq_post}%")


if __name__ == "__main__":
    main()
