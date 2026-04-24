"""Airflow client: docker exec CLI fallback since REST API returns 0 DAGs."""
from __future__ import annotations
import json
import logging
import os
import subprocess
from datetime import datetime

import requests

from .redis_cache import cached_query

logger = logging.getLogger(__name__)

AIRFLOW_URL  = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER",     "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASSWORD", "admin")
CONTAINER    = os.getenv("AIRFLOW_CONTAINER","mip_airflow")

_TIMEOUT = 6


def _run_airflow_cli(*args: str) -> str:
    try:
        result = subprocess.run(
            ["docker", "exec", CONTAINER, "airflow"] + list(args),
            capture_output=True, text=True, timeout=15,
        )
        return result.stdout
    except Exception as e:
        logger.warning(f"Airflow CLI failed: {e}")
        return ""


def _parse_dag_list(raw: str) -> list[dict]:
    lines = raw.strip().splitlines()
    dags = []
    for line in lines[2:]:  # skip header + separator
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 4:
            dags.append({
                "dag_id":   parts[0],
                "filepath": parts[1],
                "owner":    parts[2],
                "is_paused": parts[3].strip().lower() == "true",
            })
    return dags


def _rest_list_dags() -> list[dict] | None:
    try:
        r = requests.get(f"{AIRFLOW_URL}/api/v1/dags?limit=50",
                         auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            if data.get("total_entries", 0) > 0:
                return data.get("dags", [])
    except Exception:
        pass
    return None


def list_dags() -> list[dict]:
    def _fetch():
        rest = _rest_list_dags()
        if rest:
            return rest
        raw = _run_airflow_cli("dags", "list")
        return _parse_dag_list(raw)
    return cached_query("ui:airflow:dag_list", _fetch, ttl=10)


def list_dag_runs(dag_id: str, limit: int = 5) -> list[dict]:
    def _fetch():
        try:
            r = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-execution_date"},
                auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=_TIMEOUT,
            )
            if r.status_code == 200:
                return r.json().get("dag_runs", [])
        except Exception:
            pass
        raw = _run_airflow_cli("dags", "list-runs", "-d", dag_id, "--no-backfill")
        return _parse_run_list(raw, dag_id)
    return cached_query(f"ui:airflow:runs:{dag_id}", _fetch, ttl=10)


def _parse_run_list(raw: str, dag_id: str) -> list[dict]:
    runs = []
    for line in raw.strip().splitlines()[2:]:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 4:
            runs.append({
                "dag_id":        dag_id,
                "dag_run_id":    parts[0],
                "execution_date":parts[1],
                "state":         parts[3],
            })
    return runs


def trigger_dag(dag_id: str, conf: dict = {}) -> bool:
    try:
        r = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            json={"conf": conf},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=_TIMEOUT,
        )
        if r.status_code in (200, 201):
            from .redis_cache import invalidate
            invalidate("ui:airflow:*")
            return True
    except Exception as e:
        logger.warning(f"Trigger DAG {dag_id} failed: {e}")
    return False


def get_task_logs(dag_id: str, run_id: str, task_id: str, attempt: int = 1) -> str:
    try:
        r = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{attempt}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10,
        )
        if r.status_code == 200:
            return r.text[-4000:]  # last 4k chars
    except Exception:
        pass
    raw = _run_airflow_cli("tasks", "logs", dag_id, task_id, "2026-01-01")
    return raw[-4000:] if raw else "No logs available."


def get_running_dags() -> list[dict]:
    def _fetch():
        try:
            r = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/~/dagRuns",
                params={"state": "running", "limit": 10},
                auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=_TIMEOUT,
            )
            if r.status_code == 200:
                return r.json().get("dag_runs", [])
        except Exception:
            pass
        return []
    return cached_query("ui:airflow:running", _fetch, ttl=10)
