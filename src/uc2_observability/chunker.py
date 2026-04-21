"""
UC2 Observability Layer — Postgres → ChromaDB Chunker

Periodically reads new audit_events rows from Postgres (since the last
processed id, tracked in /tmp/chunker_cursor.txt), embeds each event as
a readable text chunk using sentence-transformers all-MiniLM-L6-v2, and
upserts into a ChromaDB collection called `audit_corpus`.

Runs every 5 minutes (simple sleep loop; replace with APScheduler or
systemd timer in production).
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import chromadb
import psycopg2
import psycopg2.extras
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

PG_DSN = "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD"
CHROMA_HOST = "localhost"
CHROMA_PORT = 8000
CHROMA_COLLECTION = "audit_corpus"
EMBED_MODEL = "all-MiniLM-L6-v2"
CURSOR_FILE = Path("/tmp/chunker_cursor.txt")
BATCH_SIZE = 200
INTERVAL_SECONDS = 300  # 5 minutes


# ── formatting ─────────────────────────────────────────────────────────────────

def _format_event(row: dict) -> str:
    """
    Convert a Postgres audit_events row into a human-readable text chunk
    suitable for embedding and RAG retrieval.
    """
    run_id = row.get("run_id", "unknown")
    source = row.get("source", "unknown")
    event_type = row.get("event_type", "unknown")
    status = row.get("status") or ""
    ts = row.get("ts", "")
    payload_raw = row.get("payload") or "{}"

    try:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
    except (json.JSONDecodeError, TypeError):
        payload = {}

    lines = [
        f"[{event_type}] run_id={run_id} source={source}",
        f"  timestamp: {ts}",
    ]
    if status:
        lines.append(f"  status: {status}")

    # Include selected payload fields verbosely
    for field in ("block", "rows_in", "rows_out", "null_rates", "duration_ms",
                  "reason", "row_hash", "cluster_id", "members", "canonical",
                  "error", "message"):
        val = payload.get(field)
        if val is not None:
            if isinstance(val, (dict, list)):
                val = json.dumps(val, separators=(",", ":"))
            lines.append(f"  {field}: {val}")

    return "\n".join(lines)


# ── cursor helpers ─────────────────────────────────────────────────────────────

def _read_cursor() -> int:
    if CURSOR_FILE.exists():
        try:
            return int(CURSOR_FILE.read_text().strip())
        except (ValueError, OSError):
            pass
    return 0


def _write_cursor(value: int) -> None:
    CURSOR_FILE.write_text(str(value))


# ── core chunker ───────────────────────────────────────────────────────────────

class Chunker:
    """Embeds new audit_events and upserts them into ChromaDB."""

    def __init__(self) -> None:
        logger.info("Loading embedding model %s …", EMBED_MODEL)
        self.model = SentenceTransformer(EMBED_MODEL)

        logger.info("Connecting to ChromaDB %s:%s …", CHROMA_HOST, CHROMA_PORT)
        self.chroma = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        self.collection = self.chroma.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info("ChromaDB collection '%s' ready.", CHROMA_COLLECTION)

    def _fetch_new_events(self, pg_conn, last_id: int) -> list[dict]:
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, run_id, source, event_type, status,
                       EXTRACT(EPOCH FROM ts) AS ts, payload
                FROM   audit_events
                WHERE  id > %s
                ORDER  BY id ASC
                LIMIT  %s
                """,
                (last_id, BATCH_SIZE),
            )
            return [dict(r) for r in cur.fetchall()]

    def _upsert_batch(self, rows: list[dict]) -> None:
        if not rows:
            return

        ids: list[str] = []
        documents: list[str] = []
        metadatas: list[dict] = []

        for row in rows:
            doc_id = f"audit_{row['id']}"
            text = _format_event(row)
            ids.append(doc_id)
            documents.append(text)
            metadatas.append({
                "run_id":     str(row.get("run_id", "")),
                "source":     str(row.get("source", "")),
                "event_type": str(row.get("event_type", "")),
                "pg_id":      int(row["id"]),
                "ts":         float(row.get("ts") or 0.0),
            })

        embeddings = self.model.encode(documents, show_progress_bar=False).tolist()

        self.collection.upsert(
            ids=ids,
            documents=documents,
            embeddings=embeddings,
            metadatas=metadatas,
        )
        logger.info("Upserted %d chunks into ChromaDB collection '%s'.",
                    len(rows), CHROMA_COLLECTION)

    def run_once(self) -> int:
        """
        Process one batch of new events.
        Returns the number of events processed.
        """
        last_id = _read_cursor()
        pg_conn = psycopg2.connect(PG_DSN)
        try:
            rows = self._fetch_new_events(pg_conn, last_id)
            if not rows:
                logger.debug("No new audit_events since id=%d.", last_id)
                return 0

            self._upsert_batch(rows)
            new_cursor = rows[-1]["id"]
            _write_cursor(new_cursor)
            logger.info("Cursor advanced to %d (%d events processed).",
                        new_cursor, len(rows))
            return len(rows)
        finally:
            pg_conn.close()

    def run_forever(self) -> None:
        """Main loop: run every INTERVAL_SECONDS."""
        logger.info("Chunker running every %ds …", INTERVAL_SECONDS)
        while True:
            try:
                self.run_once()
            except Exception as exc:
                logger.error("Chunker error: %s", exc)
            time.sleep(INTERVAL_SECONDS)


# ── entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    Chunker().run_forever()


if __name__ == "__main__":
    main()
