"""EDA (exploratory data analysis) library for Marketplace Intelligence Platform.

Public surface:
- load_bronze / load_silver / load_gold / load_run_logs — data loaders
- compute_stats — aggregate a triple into an EDAStats dataclass
- EDAStats — dataclass holding shape, nulls, schema diff, DQ, enrichment, categories
"""

from src.eda.report import (
    EDAStats,
    compute_stats,
    load_bronze,
    load_gold,
    load_run_logs,
    load_silver,
)

__all__ = [
    "EDAStats",
    "compute_stats",
    "load_bronze",
    "load_gold",
    "load_run_logs",
    "load_silver",
]
