"""LangGraph StateGraph + step-by-step runner for the Streamlit UI."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
from langgraph.graph import StateGraph, END

from src.agents.state import PipelineState
from src.agents.orchestrator import (
    load_source_node,
    analyze_schema_node,
    check_registry_node,
)
from src.agents.critic import critique_schema_node
from src.agents.prompts import SEQUENCE_PLANNING_PROMPT
from src.models.llm import (
    call_llm_json,
    get_orchestrator_llm,
    _UC2_AVAILABLE,
    _emit_event,
    _MetricsCollector,
    reset_llm_counter,
    get_llm_call_count,
)
from src.registry.block_registry import BlockRegistry
from src.pipeline.runner import PipelineRunner, DEFAULT_CHUNK_SIZE, NULL_RATE_COLUMNS
from src.schema.analyzer import get_domain_schema

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"


def _push_silver_audit(run_log: dict) -> None:
    """Write audit events to Postgres audit_events table. Non-fatal."""
    import json
    from datetime import datetime, timezone
    try:
        import psycopg2
        from src.uc2_observability.kafka_to_pg import PG_DSN
        conn = psycopg2.connect(PG_DSN)
        ts = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for event_type in ("run_started", "run_completed"):
                cur.execute(
                    """INSERT INTO audit_events (run_id, source, event_type, status, ts, payload)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (
                        run_log["run_id"],
                        run_log.get("source_name", "unknown"),
                        event_type,
                        run_log.get("status", "unknown"),
                        ts,
                        json.dumps(run_log),
                    ),
                )
        conn.commit()
        conn.close()
        logger.info("Postgres audit events written for run_id=%s", run_log["run_id"])
    except Exception as exc:
        logger.warning("Postgres audit write failed (non-fatal): %s", exc)


def _silver_normalize(
    df: pd.DataFrame,
    domain_schema,
    dq_weights: dict,
) -> pd.DataFrame:
    """Enforce domain schema column set and order post-block-sequence.

    Adds null-filled columns for any schema column absent from df.
    Recomputes dq_score_pre if any required non-computed column was null-filled.
    Drops columns not in domain schema.
    Returns df with exactly domain_schema.columns keys in declaration order.
    """
    canonical_cols = list(domain_schema.columns.keys())
    added_required = []

    for col in canonical_cols:
        if col not in df.columns:
            df[col] = pd.NA
            spec = domain_schema.columns[col]
            if spec.required and not spec.computed:
                added_required.append(col)

    if added_required:
        from src.blocks.dq_score import compute_dq_score
        df["dq_score_pre"] = compute_dq_score(df, dq_weights)

    return df[canonical_cols]


# ── Pipeline execution nodes ─────────────────────────────────────────


def route_after_analyze_schema(state: PipelineState) -> str:
    """Skip critique_schema when YAML mapping was loaded from Redis cache, or when Critic is disabled."""
    if state.get("cache_yaml_hit"):
        return "check_registry"
    if not state.get("with_critic", False):
        logger.info("Agent 2 (Critic) skipped — use --with-critic to enable")
        return "check_registry"
    return "critique_schema"


def plan_sequence_node(state: PipelineState) -> dict:
    """
    Agent 3: LLM selects which optional blocks to run and orders them.

    Mandatory blocks always run. Optional blocks are included only if
    beneficial for this specific source based on its schema characteristics.
    """
    if state.get("block_sequence"):
        return {}
    domain = state.get("domain", "nutrition")
    source_schema = state.get("source_schema", {})
    gaps = state.get("gaps", [])
    registry_misses = state.get("registry_misses", [])
    block_registry_hits = state.get("block_registry_hits", {})
    enable_enrichment = state.get("enable_enrichment", True)

    block_reg = BlockRegistry.instance()
    pipeline_mode = state.get("pipeline_mode") or "full"

    # Mandatory blocks per mode — always forced into the sequence
    if pipeline_mode == "silver":
        mandatory = ["dq_score_pre", "__generated__", "schema_enforce"]
        optional_names = [
            "strip_whitespace", "lowercase_brand", "remove_noise_words",
            "strip_punctuation", "extract_quantity_column",
        ]
    else:
        mandatory = ["dq_score_pre", "__generated__", "dedup_stage", "dq_score_post"]
        optional_names = [
            "strip_whitespace", "lowercase_brand", "remove_noise_words",
            "strip_punctuation", "extract_quantity_column", "enrich_stage",
        ]

    # Build metadata for mandatory and optional blocks separately
    mandatory_metadata = block_reg.get_blocks_with_metadata(mandatory)
    optional_metadata = block_reg.get_blocks_with_metadata(optional_names)

    gap_summary = {
        "gaps_detected": len(gaps),
        "block_registry_hits": block_registry_hits,
        "misses_requiring_generated_blocks": [
            g["target_column"] for g in registry_misses
        ],
    }

    compact_schema = {
        col: {"type": info.get("dtype", "unknown")}
        for col, info in source_schema.items()
    }

    model = get_orchestrator_llm()
    result = call_llm_json(
        model=model,
        messages=[
            {
                "role": "user",
                "content": SEQUENCE_PLANNING_PROMPT.format(
                    domain=domain,
                    source_schema=json.dumps(compact_schema, indent=2),
                    gap_summary=json.dumps(gap_summary, indent=2),
                    mandatory_blocks=json.dumps(mandatory_metadata, indent=2),
                    optional_blocks=json.dumps(optional_metadata, indent=2),
                ),
            }
        ],
    )

    sequence = result.get("block_sequence", [])
    reasoning = result.get("reasoning", "")
    skipped = result.get("skipped_blocks", {})

    # Force-insert any missing mandatory blocks in their correct positions
    if "dq_score_pre" not in sequence:
        sequence.insert(0, "dq_score_pre")
    if "__generated__" not in sequence:
        idx = sequence.index("dq_score_pre") + 1 if "dq_score_pre" in sequence else 0
        sequence.insert(idx, "__generated__")
    if pipeline_mode == "silver" and "schema_enforce" not in sequence:
        sequence.append("schema_enforce")
    if pipeline_mode != "silver":
        if "dedup_stage" not in sequence:
            insert_at = len(sequence) - 1 if "dq_score_post" in sequence else len(sequence)
            sequence.insert(insert_at, "dedup_stage")
        if "dq_score_post" not in sequence:
            sequence.append("dq_score_post")

    if skipped:
        for block, reason in skipped.items():
            logger.info(f"Agent 3 skipped '{block}': {reason}")

    logger.info(f"Agent 3 planned sequence ({len(sequence)} blocks): {sequence}")
    if reasoning:
        logger.info(f"Agent 3 reasoning: {reasoning}")

    result: dict = {
        "block_sequence": sequence,
        "sequence_reasoning": reasoning,
        "skipped_blocks": skipped,
    }

    # Write complete YAML cache entry now that all three agents have run.
    fingerprint = state.get("_schema_fingerprint")
    cache_client = state.get("cache_client")
    if fingerprint and cache_client is not None:
        try:
            from src.cache.client import CACHE_TTL_YAML
            import json as _json
            yaml_path = state.get("mapping_yaml_path")
            yaml_text = None
            if yaml_path:
                from pathlib import Path as _Path
                _p = _Path(yaml_path)
                if _p.exists():
                    yaml_text = _p.read_text()

            cacheable: dict = {
                "column_mapping": state.get("column_mapping", {}),
                "operations": state.get("operations", []),
                "revised_operations": state.get("revised_operations"),
                "mapping_yaml_path": yaml_path,
                "block_registry_hits": state.get("block_registry_hits", {}),
                "block_sequence": sequence,
                "sequence_reasoning": reasoning,
                "skipped_blocks": skipped,
                "enrichment_columns_to_generate": state.get("enrichment_columns_to_generate", []),
                "enrich_alias_ops": state.get("enrich_alias_ops", []),
                "gaps": state.get("gaps", []),
                "derivable_gaps": state.get("derivable_gaps", []),
                "missing_columns": state.get("missing_columns", []),
                "unresolvable_gaps": state.get("unresolvable_gaps", []),
                "mapping_warnings": state.get("mapping_warnings", []),
                "excluded_columns": state.get("excluded_columns", []),
                "validation_profile": state.get("validation_profile"),
                "__yaml_text__": yaml_text,
            }
            cache_client.set("yaml", fingerprint, _json.dumps(cacheable).encode(), ttl=CACHE_TTL_YAML)
            logger.info(f"YAML cache written (fingerprint {fingerprint})")
        except Exception as e:
            logger.warning(f"YAML cache write failed: {e}")

    return result


def run_pipeline_node(state: PipelineState) -> dict:
    """Execute the block sequence on the working DataFrame."""
    if state.get("working_df") is not None:
        return {}
    block_registry = BlockRegistry.instance()
    runner = PipelineRunner(block_registry)

    domain = state.get("domain", "nutrition")
    unified = get_domain_schema(domain)
    block_sequence = state.get("block_sequence") or block_registry.get_default_sequence(
        domain=domain,
        unified_schema=unified,
        enable_enrichment=state.get("enable_enrichment", True),
    )

    config = {
        "dq_weights": unified.dq_weights.model_dump(),
        "domain": domain,
        "unified_schema": unified,
        "cache_client": state.get("cache_client"),
    }

    source_path = state.get("source_path")
    if source_path is None:
        raise ValueError("Missing 'source_path' in state — cannot stream data for pipeline execution.")
    source_name = state.get("resolved_source_name") or Path(source_path).stem
    if "*" in source_name:
        source_name = "glob"
    column_mapping = state.get("column_mapping", {})

    # UC2: generate run_id, thread into config, reset LLM counter
    run_id = str(uuid4())
    _block_start_time = time.monotonic()
    reset_llm_counter()
    config["run_id"] = run_id
    config["source_name"] = source_name
    config["pipeline_mode"] = state.get("pipeline_mode") or "full"

    # UC2: emit run_started
    if _UC2_AVAILABLE:
        try:
            _emit_event({
                "event_type": "run_started",
                "run_id": run_id,
                "source": source_name,
                "ts": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.warning(f"UC2 run_started emit failed: {e}")

    _run_status = "success"
    result_df = pd.DataFrame()
    audit_log: list[dict] = []

    try:
        result_df, audit_log = runner.run_chunked(
            source_path=source_path,
            block_sequence=block_sequence,
            column_mapping=column_mapping,
            config=config,
            chunk_size=state.get("chunk_size", DEFAULT_CHUNK_SIZE),
            sep=state.get("source_sep", ","),
        )
        result_df = _silver_normalize(result_df, unified, config["dq_weights"])
    except Exception:
        _run_status = "failed"
        raise
    finally:
        # UC2: emit run_completed (fires even on failure)
        if _UC2_AVAILABLE:
            try:
                _emit_event({
                    "event_type": "run_completed",
                    "run_id": run_id,
                    "source": source_name,
                    "status": _run_status,
                    "total_rows": len(result_df),
                    "ts": datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                logger.warning(f"UC2 run_completed emit failed: {e}")

    dq_pre = (
        float(result_df["dq_score_pre"].mean())
        if "dq_score_pre" in result_df.columns
        else 0.0
    )
    dq_post = (
        float(result_df["dq_score_post"].mean())
        if "dq_score_post" in result_df.columns
        else 0.0
    )

    enrichment_stats = {}
    try:
        enrich_block = block_registry.get("llm_enrich")
        enrichment_stats = getattr(enrich_block, "last_enrichment_stats", {})
    except Exception:
        pass

    # Apply enrich aliases: copy enrichment col → required col where null
    # Runs post-enrichment (all blocks already executed), before quarantine check
    for alias in state.get("enrich_alias_ops", []):
        src, tgt = alias.get("source", ""), alias.get("target", "")
        if not src or not tgt:
            continue
        if src in result_df.columns and tgt in result_df.columns:
            result_df[tgt] = result_df[tgt].fillna(result_df[src])
        elif src in result_df.columns:
            result_df[tgt] = result_df[src].copy()

    excluded = set(state.get("excluded_columns") or [])
    validation_profile = state.get("validation_profile")

    if validation_profile:
        required_cols = sorted([
            col for col, spec in validation_profile.items()
            if spec["required"] and col not in excluded
        ])
        absent_skipped = [
            col for col, spec in validation_profile.items()
            if spec["status"] == "absent" and col in unified.required_columns and col not in excluded
        ]
        if absent_skipped:
            logger.info(
                f"Quarantine: skipping {len(absent_skipped)} structurally absent "
                f"required columns for this source: {absent_skipped}"
            )
    else:
        required_cols = sorted(unified.required_columns - excluded)

    existing_required = [c for c in required_cols if c in result_df.columns]
    missing_cols = [c for c in required_cols if c not in result_df.columns]

    if missing_cols:
        logger.warning(
            f"Schema mismatch: {len(missing_cols)} required columns missing from output: {missing_cols}"
        )
        quarantined_mask = (
            result_df[existing_required].isna().any(axis=1)
            if existing_required
            else pd.Series(False, index=result_df.index)
        )
    else:
        quarantined_mask = (
            result_df[required_cols].isna().any(axis=1)
            if required_cols
            else pd.Series(False, index=result_df.index)
        )
    quarantined_df = result_df[quarantined_mask].copy()
    clean_df = result_df[~quarantined_mask].copy()

    quarantine_reasons = []
    for idx in quarantined_df.index:
        missing = [
            c for c in required_cols
            if c in quarantined_df.columns and pd.isna(quarantined_df.at[idx, c])
        ]
        reason_str = f"Null in required field(s): {', '.join(missing)}"
        quarantine_reasons.append(
            {
                "row_idx": int(idx),
                "missing_fields": missing,
                "reason": reason_str,
            }
        )
        # UC2: emit quarantine event per rejected row (fire-and-forget)
        if _UC2_AVAILABLE and missing:
            try:
                row = quarantined_df.loc[idx]
                offending_field = missing[0]

                def _safe(v: object) -> object:
                    import pandas as _pd
                    import numpy as _np
                    if _pd.isna(v) if not isinstance(v, (list, dict)) else False:
                        return None
                    if isinstance(v, _np.generic):
                        return v.item()
                    return v

                row_data: dict = {
                    "product_name": _safe(row.get("product_name") if hasattr(row, "get") else getattr(row, "product_name", None)),
                    "brand_name": _safe(row.get("brand_name") if hasattr(row, "get") else getattr(row, "brand_name", None)),
                    "ingredients": _safe(row.get("ingredients") if hasattr(row, "get") else getattr(row, "ingredients", None)),
                }
                if offending_field not in row_data:
                    row_data[offending_field] = _safe(row[offending_field] if offending_field in row.index else None)

                row_hash = hashlib.sha256(str(row.to_dict()).encode()).hexdigest()[:16]
                _emit_event({
                    "event_type": "quarantine",
                    "run_id": run_id,
                    "source": source_name,
                    "row_hash": row_hash,
                    "row_data": row_data,
                    "reason": reason_str,
                    "ts": datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                logger.warning(f"UC2 quarantine emit failed: {e}")

    if len(quarantined_df) > 0:
        logger.info(
            f"Quarantine: {len(quarantined_df)} rows failed post-enrichment validation"
        )
        for _col in required_cols:
            if _col in result_df.columns:
                _nulls = int(result_df[_col].isna().sum())
                if _nulls > 0:
                    _vstatus = (validation_profile or {}).get(_col, {}).get("status", "unknown")
                    logger.info(
                        f"Quarantine trigger: '{_col}' (status={_vstatus}) — {_nulls} null rows"
                    )
    logger.info(
        f"Validation: {len(clean_df)} rows passed, {len(quarantined_df)} rows quarantined"
    )

    # UC2: emit dedup_cluster events for Stage B clusters
    if _UC2_AVAILABLE:
        try:
            dedup_block = block_registry.get("fuzzy_deduplicate")
            for cluster in getattr(dedup_block, "last_clusters", []):
                _emit_event({
                    "event_type": "dedup_cluster",
                    "run_id": run_id,
                    "cluster_id": str(cluster.get("cluster_id")),
                    "members": cluster.get("member_product_names", []),
                    "canonical": {
                        "product_name": cluster.get("canonical_product_name"),
                        "brand_name": cluster.get("canonical_brand_name"),
                    },
                    "merge_decisions": {
                        "size": cluster.get("size"),
                        "dedup_key": cluster.get("dedup_key"),
                    },
                    "ts": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.warning(f"UC2 dedup_cluster emit failed: {e}")

    return {
        "working_df": clean_df,
        "quarantined_df": quarantined_df,
        "quarantine_reasons": quarantine_reasons,
        "block_sequence": block_sequence,
        "audit_log": audit_log,
        "enrichment_stats": enrichment_stats,
        "dq_score_pre": round(dq_pre, 2),
        "dq_score_post": round(dq_post, 2),
        "_run_id": run_id,
    }


def save_output_node(state: PipelineState) -> dict:
    """Save the final DataFrame — Parquet to GCS Silver (silver mode) or CSV locally (full mode)."""
    from src.uc2_observability.log_writer import RunLogWriter
    from src.uc2_observability.metrics_exporter import MetricsExporter
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_path = state.get("source_path", "unknown")
    source_name = state.get("resolved_source_name") or Path(source_path).stem
    if "*" in source_name:
        source_name = Path(source_path.replace("*", "")).parent.name or "unknown"
    pipeline_mode = state.get("pipeline_mode") or "full"
    _run_start = state.get("_run_start_time")

    # Derive date partition and logical source name from GCS URI
    # Bronze URI pattern: gs://bucket/{source}/{YYYY}/{MM}/{DD}/part_NNNN.jsonl
    # For glob paths, resolved_source_name is already the correct folder name.
    _silver_date: str | None = None
    _silver_source: str = source_name  # already resolved above
    if pipeline_mode == "silver":
        import re as _re
        _m = _re.search(r"gs://[^/]+/([^/]+)/(\d{4}/\d{2}/\d{2})", source_path)
        if _m:
            # Override source only when no resolved name (single-file, non-glob paths)
            if not state.get("resolved_source_name"):
                _silver_source = _m.group(1)
            _silver_date = _m.group(2)
        elif state.get("resolved_source_name"):
            # Glob path: date is embedded deeper; extract it from anywhere in the URI
            _dm = _re.search(r"/(\d{4}/\d{2}/\d{2})(?:/|$)", source_path)
            if _dm:
                _silver_date = _dm.group(1)

    output_path = OUTPUT_DIR / f"{source_name}_unified.csv"
    silver_uri: str | None = None
    quarantine_uri: str | None = None

    try:
        df = state.get("working_df")
        if df is None:
            raise ValueError("Missing 'working_df' in state — run_pipeline_node did not complete successfully.")

        quarantined_df = state.get("quarantined_df")

        if pipeline_mode == "silver":
            from src.pipeline.writers.gcs_silver_writer import GCSSilverWriter
            writer = GCSSilverWriter()
            silver_uri = writer.write(df, source_name=_silver_source, date=_silver_date, chunk_idx=0)
            if _silver_date:
                writer.update_watermark(_silver_source, _silver_date)
            if quarantined_df is not None and len(quarantined_df) > 0:
                quarantine_source = f"{_silver_source}_quarantine"
                quarantine_uri = writer.write(quarantined_df, source_name=quarantine_source, date=_silver_date, chunk_idx=0)
                logger.info(f"Quarantine: {len(quarantined_df)} rows → {quarantine_uri}")
        else:
            df.to_csv(output_path, index=False)
            logger.info(f"Output saved to {output_path} ({len(df)} rows)")
            if quarantined_df is not None and len(quarantined_df) > 0:
                q_path = OUTPUT_DIR / f"{source_name}_quarantined.csv"
                quarantined_df.to_csv(q_path, index=False)
                logger.info(f"Quarantine: {len(quarantined_df)} rows → {q_path}")

        # Silver local parquet write + Gold rebuild (all pipeline modes)
        domain = state.get("domain", "nutrition")
        silver_local_dir = OUTPUT_DIR / "silver" / domain
        silver_local_dir.mkdir(parents=True, exist_ok=True)
        silver_local_path = silver_local_dir / f"{source_name}.parquet"
        df.to_parquet(silver_local_path, index=False)
        logger.info("Silver: %d rows → %s", len(df), silver_local_path)

        silver_files = sorted(silver_local_dir.glob("*.parquet"))
        gold_path: str | None = None
        if silver_files:
            gold_df = pd.concat([pd.read_parquet(p) for p in silver_files], ignore_index=True)
            gold_dir = OUTPUT_DIR / "gold"
            gold_dir.mkdir(parents=True, exist_ok=True)
            gold_path = str(gold_dir / f"{domain}.parquet")
            gold_df.to_parquet(gold_path, index=False)
            logger.info("Gold: %d rows → %s", len(gold_df), gold_path)
        else:
            logger.warning("No Silver parquet files for domain '%s' — Gold write skipped", domain)

        cache_client = state.get("cache_client")
        if cache_client is not None:
            cache_client.get_stats().log_all()

        # UC2: push Prometheus metrics (legacy MetricsCollector)
        if _UC2_AVAILABLE:
            try:
                source_df = state.get("source_df")
                rows_in = len(source_df) if source_df is not None else 0
                rows_out = len(df)

                null_rate = float(
                    df[NULL_RATE_COLUMNS].isna().mean().mean()
                ) if all(c in df.columns for c in NULL_RATE_COLUMNS) else 0.0

                enrichment_stats = state.get("enrichment_stats") or {}
                llm_calls = get_llm_call_count()

                try:
                    dedup_block = BlockRegistry.instance().get("fuzzy_deduplicate")
                    dedup_rate = getattr(dedup_block, "last_dedup_rate", 0.0)
                except Exception:
                    dedup_rate = 0.0

                quarantine_rows = len(quarantined_df) if quarantined_df is not None else 0

                dq_score_pre = float(state.get("dq_score_pre") or 0.0)
                dq_score_post = float(state.get("dq_score_post") or 0.0)

                run_start_t = _run_start or 0.0
                block_duration = time.monotonic() - run_start_t if run_start_t else 0.0

                metrics = {
                    "rows_in": rows_in,
                    "rows_out": rows_out,
                    "dq_score_pre": dq_score_pre,
                    "dq_score_post": dq_score_post,
                    "dq_delta": round(dq_score_post - dq_score_pre, 4),
                    "null_rate": round(null_rate, 4),
                    "dedup_rate": round(float(dedup_rate), 4),
                    "s1_count": int(enrichment_stats.get("deterministic", 0)),
                    "s2_count": int(enrichment_stats.get("embedding", 0)),
                    "s3_count": 0,
                    "s4_count": int(enrichment_stats.get("llm", 0)),
                    "cost_usd": round(llm_calls * 0.002, 6),
                    "llm_calls": llm_calls,
                    "quarantine_rows": quarantine_rows,
                    "block_duration_seconds": round(block_duration, 3),
                }

                _MetricsCollector().push(
                    run_id=state.get("_run_id", "unknown"),
                    source=source_name,
                    metrics_dict=metrics,
                )
            except Exception as e:
                logger.warning(f"UC2 MetricsCollector.push failed: {e}")

        # Write structured run log
        log_path = RunLogWriter().save(state, status="success", start_time=_run_start)

        # Push Prometheus metrics via Pushgateway + Postgres audit_events
        if log_path is not None:
            try:
                import json as _json
                run_log_dict = _json.loads(log_path.read_text(encoding="utf-8"))
                MetricsExporter().push(run_log_dict)
            except Exception as e:
                logger.warning(f"MetricsExporter.push failed: {e}")
            try:
                import json as _json2
                _rl = _json2.loads(log_path.read_text(encoding="utf-8"))
                _push_silver_audit(_rl)
            except Exception as e:
                logger.warning(f"Silver audit push failed: {e}")

    except Exception as e:
        try:
            _partial_log_path = RunLogWriter().save(state, status="partial", error=str(e), start_time=_run_start)
            if _partial_log_path is not None:
                import json as _json3
                _push_silver_audit(_json3.loads(_partial_log_path.read_text(encoding="utf-8")))
        except Exception as inner:
            logger.warning(f"RunLogWriter.save (partial) failed: {inner}")
        raise

    return {
        "output_path": silver_uri or str(output_path),
        "silver_output_uri": silver_uri,
        "quarantine_output_uri": quarantine_uri,
        "silver_local_path": str(silver_local_path),
        "gold_path": gold_path,
    }



# ── Step-by-step runner (for Streamlit UI) ───────────────────────────

NODE_MAP = {
    "load_source": load_source_node,
    "analyze_schema": analyze_schema_node,
    "critique_schema": critique_schema_node,
    "check_registry": check_registry_node,
    "plan_sequence": plan_sequence_node,
    "run_pipeline": run_pipeline_node,
    "save_output": save_output_node,
}


def run_step(step_name: str, state: dict) -> dict:
    """
    Run a single pipeline step by name. Used by the Streamlit wizard
    to execute nodes sequentially with HITL gates in between.
    """
    if step_name not in NODE_MAP:
        raise KeyError(f"Unknown step: {step_name}. Available: {list(NODE_MAP.keys())}")

    node_fn = NODE_MAP[step_name]
    updates = node_fn(state)
    state.update(updates)
    return state


# ── Full graph builder (for CLI / non-interactive use) ───────────────


def build_graph() -> StateGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("load_source", load_source_node)
    graph.add_node("analyze_schema", analyze_schema_node)
    graph.add_node("critique_schema", critique_schema_node)
    graph.add_node("check_registry", check_registry_node)
    graph.add_node("plan_sequence", plan_sequence_node)
    graph.add_node("run_pipeline", run_pipeline_node)
    graph.add_node("save_output", save_output_node)

    graph.add_edge("load_source", "analyze_schema")
    graph.add_conditional_edges(
        "analyze_schema",
        route_after_analyze_schema,
        {"critique_schema": "critique_schema", "check_registry": "check_registry"},
    )
    graph.add_edge("critique_schema", "check_registry")
    graph.add_edge("check_registry", "plan_sequence")
    graph.add_edge("plan_sequence", "run_pipeline")
    graph.add_edge("run_pipeline", "save_output")
    graph.add_edge("save_output", END)

    graph.set_entry_point("load_source")
    return graph.compile()
