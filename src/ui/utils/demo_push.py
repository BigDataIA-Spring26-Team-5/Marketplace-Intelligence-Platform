"""Push a 50-row USDA demo slice to GCS Bronze for pipeline testing."""
from __future__ import annotations
import json
import subprocess
import tempfile
from datetime import date
from pathlib import Path

_SOURCE_URI  = "gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl"
_DEST_PREFIX = "gs://mip-bronze-2024/usda"
_N_ROWS      = 50


def push_demo_bronze(n_rows: int = _N_ROWS) -> tuple[bool, str, str]:
    """
    Copy n_rows rows from existing Bronze USDA partition to today's demo partition.
    Returns (ok, gcs_uri, message).
    """
    today = date.today().strftime("%Y/%m/%d")
    dest_uri = f"{_DEST_PREFIX}/{today}/demo_push_{n_rows}.jsonl"

    # Stream n_rows lines from source Bronze file
    cat = subprocess.run(
        ["gsutil", "cat", _SOURCE_URI],
        capture_output=True, text=True, timeout=60,
    )
    if cat.returncode != 0:
        return False, "", f"gsutil cat failed: {cat.stderr.strip()}"

    lines = [l for l in cat.stdout.splitlines() if l.strip()][:n_rows]
    if not lines:
        return False, "", "No rows returned from Bronze source"

    # Write temp JSONL and push
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write("\n".join(lines) + "\n")
        tmp_path = f.name

    try:
        cp = subprocess.run(
            ["gsutil", "cp", tmp_path, dest_uri],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            return False, "", f"gsutil cp failed: {cp.stderr.strip()}"
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return True, dest_uri, f"Pushed {len(lines)} rows → {dest_uri}"
