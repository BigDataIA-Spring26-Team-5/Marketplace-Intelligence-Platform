"""Redis cache client with graceful degradation and per-prefix SHA-256 keys."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Optional

from src.cache.stats import CacheStats

logger = logging.getLogger(__name__)

CACHE_TTL_YAML = int(os.environ.get("CACHE_TTL_YAML", str(60 * 60 * 24 * 30)))    # 30 days
CACHE_TTL_LLM = int(os.environ.get("CACHE_TTL_LLM", str(60 * 60 * 24 * 7)))       # 7 days
CACHE_TTL_EMB = int(os.environ.get("CACHE_TTL_EMB", str(60 * 60 * 24 * 30)))      # 30 days
CACHE_TTL_DEDUP = int(os.environ.get("CACHE_TTL_DEDUP", str(60 * 60 * 24 * 14)))  # 14 days

_KNOWN_PREFIXES = ("yaml", "llm", "emb", "dedup")


class CacheClient:
    """
    Thin Redis wrapper: prefix-namespaced keys, SHA-256 hashing, graceful degradation.

    All operations are no-ops when Redis is unavailable or _no_cache=True.
    Connection failure sets _available=False for the remainder of the run
    to avoid repeated timeout overhead.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, no_cache: bool = False) -> None:
        self._no_cache = no_cache
        self._available = False
        self._stats = CacheStats()
        self._pool = None

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
            logger.warning(f"Redis unavailable: {e}. Cache disabled for this run.")
            self._available = False

    def _make_key(self, prefix: str, key_input: str | list) -> str:
        if isinstance(key_input, list):
            raw = json.dumps(key_input, sort_keys=True)
        else:
            raw = key_input
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return f"{prefix}:{digest}"

    def get(self, prefix: str, key_input: str | list) -> Optional[bytes]:
        if not self._available:
            self._stats.record_miss(prefix)
            return None
        try:
            import redis
            conn = redis.Redis(connection_pool=self._pool)
            key = self._make_key(prefix, key_input)
            value = conn.get(key)
            if value is not None:
                self._stats.record_hit(prefix)
            else:
                self._stats.record_miss(prefix)
            return value
        except Exception as e:
            logger.warning(f"Cache GET error [{prefix}]: {e}")
            self._available = False
            self._stats.record_miss(prefix)
            return None

    def set(self, prefix: str, key_input: str | list, value: bytes, ttl: int) -> bool:
        if not self._available:
            return False
        try:
            import redis
            conn = redis.Redis(connection_pool=self._pool)
            key = self._make_key(prefix, key_input)
            conn.set(key, value, ex=ttl)
            return True
        except Exception as e:
            logger.warning(f"Cache SET error [{prefix}]: {e}")
            self._available = False
            return False

    def delete(self, prefix: str, key_input: str | list) -> bool:
        if not self._available:
            return False
        try:
            import redis
            conn = redis.Redis(connection_pool=self._pool)
            key = self._make_key(prefix, key_input)
            conn.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Cache DELETE error [{prefix}]: {e}")
            return False

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
