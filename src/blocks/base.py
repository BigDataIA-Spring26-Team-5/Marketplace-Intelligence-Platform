"""Abstract base class for all transformation blocks."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Block(ABC):
    """
    Base class for ETL transformation blocks.

    Every block takes a DataFrame + config, returns a transformed DataFrame,
    and produces an audit entry for the observability layer.
    """

    name: str = "unnamed"
    domain: str = "all"  # "all", "nutrition", "safety", "pricing"
    description: str = ""
    inputs: list[str] = []   # column names / state keys this block reads
    outputs: list[str] = []  # column names / state keys this block produces

    @abstractmethod
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        """Transform the dataframe. Must return the modified dataframe."""
        ...

    def audit_entry(self, rows_in: int, rows_out: int, extra: dict | None = None) -> dict:
        """Produce a standard audit log entry."""
        entry = {
            "block": self.name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_delta": rows_out - rows_in,
        }
        if extra:
            entry.update(extra)
        return entry
