"""
Re-push UC2 metrics to Prometheus Pushgateway from Postgres audit_events.

Sources (in priority order):
  1. Postgres audit_events — richest, has all run_completed payloads
  2. Run log JSON files    — fallback for runs not yet in Postgres

Usage:
    python scripts/repush_uc2_metrics.py
    python scripts/repush_uc2_metrics.py --logs-dir /custom/run_logs
    python scripts/repush_uc2_metrics.py --pg-only
    python scripts/repush_uc2_metrics.py --logs-only
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PG_DSN        = "host=localhost port=5432 dbname=uc2 user=mip password=mip_pass"
DEFAULT_LOGS  = ROOT / "output" / "run_logs"


def _load_from_postgres() -> list[dict]:
    """Load all run_completed payloads from Postgres audit_events."""
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        cur.execute("""
            SELECT payload FROM audit_events
            WHERE event_type = 'run_completed'
              AND payload IS NOT NULL
              AND payload::text != 'null'
              AND payload->>'backfilled' IS NULL
            ORDER BY ts ASC
        """)
        rows = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()
        logger.info("Loaded %d run_completed payloads from Postgres", len(rows))
        return rows
    except Exception as exc:
        logger.warning("Postgres load failed: %s", exc)
        return []


def _load_from_logs(logs_dir: Path) -> list[dict]:
    """Load run log JSON files, skip runs already covered by Postgres."""
    logs = []
    for path in sorted(logs_dir.glob("run_*.json")):
        try:
            with open(path) as f:
                d = json.load(f)
            if d.get("source_name") and d.get("run_id"):
                logs.append(d)
        except Exception as exc:
            logger.warning("Could not read %s: %s", path.name, exc)
    logger.info("Loaded %d run log JSON files", len(logs))
    return logs


def _push_block_dq(exporter, run_log: dict) -> int:
    """Push per-block DQ scores from audit_log entries."""
    from src.uc2_observability.metrics_collector import MetricsCollector
    collector = MetricsCollector()
    run_id = run_log.get("run_id", "unknown")
    source = run_log.get("source_name") or run_log.get("source", "unknown")
    pushed = 0
    for chunk in run_log.get("audit_log", []):
        entries = chunk.get("logs", []) if isinstance(chunk, dict) else []
        for seq, entry in enumerate(entries):
            block = entry.get("block", "")
            dq = entry.get("dq_score") or entry.get("dq_score_post")
            rows = entry.get("rows_out") or entry.get("rows_in") or 0
            if dq is not None and block:
                try:
                    collector.push_block_dq(
                        run_id=run_id, source=source,
                        block_name=block, block_seq=seq,
                        dq_score=float(dq), rows=int(rows),
                    )
                    pushed += 1
                except Exception:
                    pass
    return pushed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs-dir", type=Path, default=DEFAULT_LOGS)
    parser.add_argument("--pg-only",   action="store_true", help="Only use Postgres")
    parser.add_argument("--logs-only", action="store_true", help="Only use run log JSON files")
    args = parser.parse_args()

    from src.uc2_observability.metrics_exporter import MetricsExporter
    exporter = MetricsExporter()

    # Collect run logs from both sources, deduplicate by run_id
    seen_run_ids: set[str] = set()
    all_logs: list[dict] = []

    if not args.logs_only:
        for log in _load_from_postgres():
            rid = log.get("run_id", "")
            if rid and rid not in seen_run_ids:
                seen_run_ids.add(rid)
                all_logs.append(log)

    if not args.pg_only and args.logs_dir.exists():
        for log in _load_from_logs(args.logs_dir):
            rid = log.get("run_id", "")
            if rid and rid not in seen_run_ids:
                seen_run_ids.add(rid)
                all_logs.append(log)

    if not all_logs:
        logger.error("No run logs found in Postgres or %s", args.logs_dir)
        sys.exit(1)

    logger.info("Pushing %d unique runs to Pushgateway…", len(all_logs))

    ok = failed = block_ok = 0
    sources_seen: set[str] = set()

    for run_log in all_logs:
        source = run_log.get("source_name") or run_log.get("source", "unknown")
        run_id = run_log.get("run_id", "?")
        sources_seen.add(source)

        success = exporter.push(run_log)
        if success:
            ok += 1
            block_ok += _push_block_dq(exporter, run_log)
            logger.info("  ✓ %-45s source=%-22s rows_in=%-8s dq_post=%s",
                        run_id, source,
                        run_log.get("rows_in", "?"),
                        round(float(run_log.get("dq_score_post") or 0), 3))
        else:
            failed += 1
            logger.warning("  ✗ %-45s source=%-22s  PUSH FAILED", run_id, source)

    print(f"\n{'='*60}")
    print(f"Re-pushed {ok} runs  ({failed} failed)  |  {block_ok} block-DQ points")
    print(f"Sources:  {sorted(sources_seen)}")
    print(f"Prometheus scrapes Pushgateway every ~15s.")
    print(f"Refresh Grafana after 20 seconds.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
