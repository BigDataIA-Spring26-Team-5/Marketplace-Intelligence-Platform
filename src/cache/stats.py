"""Per-run cache hit/miss accumulator."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class CacheStats:
    """Accumulates cache hit/miss counts per prefix for one pipeline run."""

    def __init__(self) -> None:
        self._counts: dict[str, dict[str, int]] = {}

    def record_hit(self, prefix: str) -> None:
        self._counts.setdefault(prefix, {"hits": 0, "misses": 0})["hits"] += 1

    def record_miss(self, prefix: str) -> None:
        self._counts.setdefault(prefix, {"hits": 0, "misses": 0})["misses"] += 1

    def summary(self) -> dict[str, dict]:
        result = {}
        for prefix, counts in self._counts.items():
            hits = counts["hits"]
            misses = counts["misses"]
            total = hits + misses
            hit_rate = round(hits / total * 100, 1) if total > 0 else 0.0
            result[prefix] = {"hits": hits, "misses": misses, "hit_rate_pct": hit_rate}
        return result

    def log_all(self) -> None:
        for prefix, stats in self.summary().items():
            logger.info(
                f"Cache {prefix}: {stats['hits']} hits, {stats['misses']} misses "
                f"({stats['hit_rate_pct']}% hit rate)"
            )
