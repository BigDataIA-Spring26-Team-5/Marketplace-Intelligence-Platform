"""Service health checks for the DataForge topbar."""
from __future__ import annotations
import os
import socket
import logging

import requests

from .redis_cache import cached_query

logger = logging.getLogger(__name__)

REDIS_HOST   = os.getenv("REDIS_HOST",        "localhost")
PG_HOST      = os.getenv("PG_HOST",           "localhost")
PG_PORT      = int(os.getenv("PG_PORT",       "5432"))
KAFKA_HOST   = os.getenv("KAFKA_HOST",        "localhost")
KAFKA_PORT   = int(os.getenv("KAFKA_PORT",    "9092"))
CHROMA_URL   = os.getenv("CHROMA_URL",        "http://localhost:8000")
MLFLOW_URL   = os.getenv("MLFLOW_TRACKING_URI","http://localhost:5000")
GRAFANA_URL  = os.getenv("GRAFANA_BASE_URL",  "http://localhost:3000")


def _tcp_ok(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _http_ok(url: str, timeout: float = 2.0) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False


def _check_services() -> dict[str, str]:
    results: dict[str, str] = {}

    # Redis
    results["Redis"] = "ok" if _tcp_ok(REDIS_HOST, 6379) else "error"

    # Postgres
    results["Postgres"] = "ok" if _tcp_ok(PG_HOST, PG_PORT) else "error"

    # Kafka
    results["Kafka"] = "ok" if _tcp_ok(KAFKA_HOST, KAFKA_PORT) else "warn"

    # ChromaDB
    results["ChromaDB"] = "ok" if _http_ok(f"{CHROMA_URL}/api/v2/heartbeat") else "error"

    # MLflow
    results["MLflow"] = "ok" if _http_ok(f"{MLFLOW_URL}/health") else "warn"

    # Grafana
    results["Grafana"] = "ok" if _http_ok(f"{GRAFANA_URL}/api/health") else "warn"

    return results


def check_all_services() -> dict[str, str]:
    return cached_query("ui:health:all", _check_services, ttl=15)


def count_active_runs() -> int:
    try:
        from src.uc2_observability.log_store import RunLogStore
        from datetime import datetime, timezone, timedelta
        store = RunLogStore()
        logs = store.load_all()
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        running = [r for r in logs if r.get("status") == "running"
                   and r.get("timestamp", "") >= cutoff]
        return len(running)
    except Exception:
        return 0
