"""Redis cache layer for DataForge Streamlit app."""
from __future__ import annotations
import json
import logging
from typing import Any, Callable

import streamlit as st

logger = logging.getLogger(__name__)

import os
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@st.cache_resource
def get_redis():
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                        decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        return None


def cached_query(key: str, fetch_fn: Callable, ttl: int = 30) -> Any:
    r = get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    result = fetch_fn()
    if r and result is not None:
        try:
            r.setex(key, ttl, json.dumps(result, default=str))
        except Exception:
            pass
    return result


def invalidate(pattern: str) -> None:
    r = get_redis()
    if not r:
        return
    try:
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)
    except Exception:
        pass


def redis_ok() -> bool:
    r = get_redis()
    if not r:
        return False
    try:
        r.ping()
        return True
    except Exception:
        return False
