"""
UC2 Observability Layer — Kafka → Postgres Consumer

Reads JSON events from the `pipeline.events` Kafka topic and demuxes
them into the correct Postgres table based on `event_type`:

  run_started / run_completed  →  audit_events
  block_start / block_end      →  block_trace
  quarantine                   →  quarantine_rows
  dedup_cluster                →  dedup_clusters

Runs forever; reconnects on Kafka or Postgres errors with exponential
back-off (up to 60 s).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

import os
KAFKA_BOOTSTRAP = os.getenv("UC2_KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "pipeline.events"
KAFKA_GROUP = "uc2-kafka-to-pg"

PG_DSN = os.getenv("UC2_PG_DSN", "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD")

MAX_BACKOFF = 60  # seconds


# ── SQL templates ──────────────────────────────────────────────────────────────

_INSERT_AUDIT = """
INSERT INTO audit_events (run_id, source, event_type, status, ts, payload)
VALUES (%(run_id)s, %(source)s, %(event_type)s, %(status)s,
        to_timestamp(%(ts)s), %(payload)s)
ON CONFLICT DO NOTHING;
"""

_INSERT_BLOCK_TRACE = """
INSERT INTO block_trace
    (run_id, source, block_name, event_type, rows_in, rows_out,
     null_rates, duration_ms, ts)
VALUES
    (%(run_id)s, %(source)s, %(block_name)s, %(event_type)s,
     %(rows_in)s, %(rows_out)s, %(null_rates)s, %(duration_ms)s,
     to_timestamp(%(ts)s))
ON CONFLICT DO NOTHING;
"""

_INSERT_QUARANTINE = """
INSERT INTO quarantine_rows (run_id, source, row_hash, reason, row_data, ts)
VALUES (%(run_id)s, %(source)s, %(row_hash)s, %(reason)s, %(row_data)s,
        to_timestamp(%(ts)s))
ON CONFLICT DO NOTHING;
"""

_INSERT_DEDUP = """
INSERT INTO dedup_clusters
    (run_id, source, cluster_id, canonical, members, merge_decisions, ts)
VALUES
    (%(run_id)s, %(source)s, %(cluster_id)s, %(canonical)s,
     %(members)s, %(merge_decisions)s, to_timestamp(%(ts)s))
ON CONFLICT DO NOTHING;
"""


# ── helpers ────────────────────────────────────────────────────────────────────

def _safe_json(obj: Any) -> str:
    """Serialise obj to a JSON string, or return '{}' on failure."""
    try:
        return json.dumps(obj)
    except (TypeError, ValueError):
        return "{}"


def _handle_audit(cur: Any, event: dict) -> None:
    cur.execute(_INSERT_AUDIT, {
        "run_id":     event.get("run_id", ""),
        "source":     event.get("source", ""),
        "event_type": event.get("event_type", ""),
        "status":     event.get("status", ""),
        "ts":         event.get("ts", time.time()),
        "payload":    _safe_json(event),
    })


def _handle_block_trace(cur: Any, event: dict) -> None:
    cur.execute(_INSERT_BLOCK_TRACE, {
        "run_id":      event.get("run_id", ""),
        "source":      event.get("source", ""),
        "block_name":  event.get("block", ""),
        "event_type":  event.get("event_type", ""),
        "rows_in":     event.get("rows_in", None),
        "rows_out":    event.get("rows_out", None),
        "null_rates":  _safe_json(event.get("null_rates", {})),
        "duration_ms": event.get("duration_ms", None),
        "ts":          event.get("ts", time.time()),
    })


def _handle_quarantine(cur: Any, event: dict) -> None:
    cur.execute(_INSERT_QUARANTINE, {
        "run_id":   event.get("run_id", ""),
        "source":   event.get("source", ""),
        "row_hash": event.get("row_hash", ""),
        "reason":   event.get("reason", ""),
        "row_data": _safe_json(event.get("row_data", {})),
        "ts":       event.get("ts", time.time()),
    })


def _handle_dedup(cur: Any, event: dict) -> None:
    cur.execute(_INSERT_DEDUP, {
        "run_id":          event.get("run_id", ""),
        "source":          event.get("source", ""),
        "cluster_id":      event.get("cluster_id", ""),
        "canonical":       event.get("canonical", ""),
        "members":         _safe_json(event.get("members", [])),
        "merge_decisions": _safe_json(event.get("merge_decisions", {})),
        "ts":              event.get("ts", time.time()),
    })


_HANDLERS = {
    "run_started":    _handle_audit,
    "run_completed":  _handle_audit,
    "block_start":    _handle_block_trace,
    "block_end":      _handle_block_trace,
    "quarantine":     _handle_quarantine,
    "dedup_cluster":  _handle_dedup,
}


# ── main consumer loop ─────────────────────────────────────────────────────────

def _make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        api_version=(2, 5, 0),
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=-1,   # block forever
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )


def _make_pg_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    return conn


def run_consumer() -> None:
    """
    Main entry point.  Runs forever; reconnects on any transient error.
    """
    backoff = 1
    while True:
        consumer = None
        pg_conn = None
        try:
            logger.info("Connecting to Kafka %s topic=%s …", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
            consumer = _make_consumer()
            logger.info("Connecting to Postgres …")
            pg_conn = _make_pg_conn()
            backoff = 1  # reset on successful connect

            logger.info("Consumer started — waiting for events …")
            for message in consumer:
                event = message.value
                if not isinstance(event, dict):
                    logger.warning("Skipping non-dict message: %r", event)
                    continue

                event_type = event.get("event_type", "")
                handler = _HANDLERS.get(event_type)
                if handler is None:
                    logger.debug("No handler for event_type=%r, skipping", event_type)
                    continue

                try:
                    with pg_conn.cursor() as cur:
                        handler(cur, event)
                    pg_conn.commit()
                    logger.debug("Inserted event_type=%s run_id=%s",
                                 event_type, event.get("run_id", "?"))
                except psycopg2.Error as pg_err:
                    pg_conn.rollback()
                    logger.error("Postgres insert error for event_type=%s: %s",
                                 event_type, pg_err)
                    # reconnect Postgres and continue
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = _make_pg_conn()

        except KafkaError as ke:
            logger.error("Kafka error: %s — reconnecting in %ds", ke, backoff)
        except psycopg2.OperationalError as pe:
            logger.error("Postgres connection error: %s — reconnecting in %ds", pe, backoff)
        except Exception as exc:
            logger.error("Unexpected error: %s — reconnecting in %ds", exc, backoff)
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
            if pg_conn:
                try:
                    pg_conn.close()
                except Exception:
                    pass

        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)


# ── Producer: emit_event (called by the pipeline) ─────────────────────────────

_producer = None  # module-level singleton; lazily created


def _get_producer():
    """Return a cached KafkaProducer, creating it on first call."""
    global _producer
    if _producer is None:
        from kafka import KafkaProducer as _KafkaProducer
        _producer = _KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            acks=0,           # fire-and-forget — pipeline must never block on UC2
            request_timeout_ms=30000,
            linger_ms=100,
            batch_size=16384,
            retries=0,
        )
    return _producer


def emit_event(event: dict) -> None:
    """Send a pipeline event to the pipeline.events Kafka topic.

    Called from graph.py and runner.py. Never raises — a UC2 failure
    must never crash the pipeline.
    """
    try:
        _get_producer().send(KAFKA_TOPIC, event)
    except Exception as exc:
        logger.warning("emit_event failed (event_type=%s): %s",
                       event.get("event_type", "?"), exc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_consumer()
