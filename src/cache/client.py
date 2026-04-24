"""Redis cache client with graceful degradation and per-prefix SHA-256 keys.

Falls back to a SQLite-backed store when Redis is unavailable, ensuring LLM
results are cached across runs even without a Redis instance.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional

from src.cache.stats import CacheStats

logger = logging.getLogger(__name__)

CACHE_TTL_YAML = int(os.environ.get("CACHE_TTL_YAML", str(60 * 60 * 24 * 30)))    # 30 days
CACHE_TTL_LLM = int(os.environ.get("CACHE_TTL_LLM", str(60 * 60 * 24 * 7)))       # 7 days
CACHE_TTL_EMB = int(os.environ.get("CACHE_TTL_EMB", str(60 * 60 * 24 * 30)))      # 30 days
CACHE_TTL_DEDUP = int(os.environ.get("CACHE_TTL_DEDUP", str(60 * 60 * 24 * 14)))  # 14 days

_KNOWN_PREFIXES = ("yaml", "llm", "emb", "dedup")

SQLITE_CACHE_PATH = os.environ.get(
    "SQLITE_CACHE_PATH",
    str(Path(__file__).resolve().parent.parent.parent / "output" / "cache.db"),
)


class _SQLiteCache:
    """Minimal SQLite KV store used as Redis fallback. Thread-safe via WAL mode."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    expires_at REAL NOT NULL
                )"""
            )

    def get(self, key: str) -> Optional[bytes]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value, expires_at FROM cache WHERE key = ?", (key,)
                ).fetchone()
            if row is None:
                return None
            value, expires_at = row
            if time.time() > expires_at:
                self.delete(key)
                return None
            return bytes(value)
        except Exception as exc:
            logger.warning("SQLite cache GET error [%s]: %s", key, exc)
            return None

    def set(self, key: str, value: bytes, ttl: int) -> None:
        try:
            expires_at = time.time() + ttl
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)",
                    (key, value, expires_at),
                )
        except Exception as exc:
            logger.warning("SQLite cache SET error [%s]: %s", key, exc)

    def delete(self, key: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM cache WHERE key = ?", (key,))
        except Exception as exc:
            logger.warning("SQLite cache DELETE error [%s]: %s", key, exc)

    def purge_expired(self) -> int:
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM cache WHERE expires_at < ?", (time.time(),)
                )
                return cur.rowcount
        except Exception:
            return 0


class CacheClient:
    """
    Thin Redis wrapper: prefix-namespaced keys, SHA-256 hashing, graceful degradation.

    All operations are no-ops when Redis is unavailable or _no_cache=True.
    Connection failure sets _available=False for the remainder of the run
    to avoid repeated timeout overhead.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        no_cache: bool = False,
        sqlite_fallback: bool = True,
        sqlite_path: str = SQLITE_CACHE_PATH,
    ) -> None:
        self._no_cache = no_cache
        self._available = False
        self._stats = CacheStats()
        self._pool = None
        self._sqlite: Optional[_SQLiteCache] = None

        if no_cache:
            return

        try:
            import redis
            self._pool = redis.ConnectionPool(
                host=host,
                port=port,
                db=db,
                max_connections=10,
                socket_connect_timeout=1,
                socket_timeout=1,
            )
            conn = redis.Redis(connection_pool=self._pool)
            conn.ping()
            self._available = True
            logger.info(f"Redis cache connected at {host}:{port}/{db}")
        except Exception as e:
            logger.warning(f"Redis unavailable: {e}. Falling back to SQLite cache.")
            if sqlite_fallback:
                try:
                    self._sqlite = _SQLiteCache(sqlite_path)
                    logger.info(f"SQLite cache initialised at {sqlite_path}")
                except Exception as se:
                    logger.warning(f"SQLite cache init failed: {se}. Cache disabled.")
            self._available = False

    def _make_key(self, prefix: str, key_input: str | list) -> str:
        if isinstance(key_input, list):
            raw = json.dumps(key_input, sort_keys=True)
        else:
            raw = key_input
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return f"{prefix}:{digest}"

    def get(self, prefix: str, key_input: str | list) -> Optional[bytes]:
        key = self._make_key(prefix, key_input)
        if self._available:
            try:
                import redis
                conn = redis.Redis(connection_pool=self._pool)
                value = conn.get(key)
                if value is not None:
                    self._stats.record_hit(prefix)
                    return value
                # Redis miss — fall through to SQLite if available
            except Exception as e:
                logger.warning(f"Cache GET error [{prefix}]: {e}")
                self._available = False

        if self._sqlite is not None:
            value = self._sqlite.get(key)
            if value is not None:
                self._stats.record_hit(prefix)
                return value

        self._stats.record_miss(prefix)
        return None

    def set(self, prefix: str, key_input: str | list, value: bytes, ttl: int) -> bool:
        key = self._make_key(prefix, key_input)
        if self._available:
            try:
                import redis
                conn = redis.Redis(connection_pool=self._pool)
                conn.set(key, value, ex=ttl)
                return True
            except Exception as e:
                logger.warning(f"Cache SET error [{prefix}]: {e}")
                self._available = False

        if self._sqlite is not None:
            self._sqlite.set(key, value, ttl)
            return True

        return False

    def delete(self, prefix: str, key_input: str | list) -> bool:
        key = self._make_key(prefix, key_input)
        ok = False
        if self._available:
            try:
                import redis
                conn = redis.Redis(connection_pool=self._pool)
                conn.delete(key)
                ok = True
            except Exception as e:
                logger.warning(f"Cache DELETE error [{prefix}]: {e}")
        if self._sqlite is not None:
            self._sqlite.delete(key)
            ok = True
        return ok

    def flush_all_prefixes(self) -> int:
        if not self._available:
            return 0
        deleted = 0
        try:
            import redis
            conn = redis.Redis(connection_pool=self._pool)
            for prefix in _KNOWN_PREFIXES:
                cursor = 0
                while True:
                    cursor, keys = conn.scan(cursor=cursor, match=f"{prefix}:*", count=100)
                    if keys:
                        conn.delete(*keys)
                        deleted += len(keys)
                    if cursor == 0:
                        break
            logger.info(f"Cache flush: deleted {deleted} keys across prefixes {_KNOWN_PREFIXES}")
        except Exception as e:
            logger.warning(f"Cache flush error: {e}")
        return deleted

    def get_stats(self) -> CacheStats:
        return self._stats
