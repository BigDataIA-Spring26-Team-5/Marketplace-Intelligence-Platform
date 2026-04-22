"""Config-driven async rate limiter for LLM API calls.

Loads limits from config/llm_rate_limits.yaml. Tracks a sliding 60-second
window for both RPM and TPM. Callers await acquire() before each dispatch;
on 429s, call backoff(attempt) to pause the dispatcher.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "llm_rate_limits.yaml"


def _load_config(provider: str = "anthropic") -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)[provider]


class RateLimiter:
    """Sliding-window rate limiter for a single LLM provider.

    Tracks request count and token count over a rolling 60-second window.
    acquire() blocks until both budgets have headroom. backoff(attempt) sleeps
    with exponential + jitter delay — call on 429 errors.
    """

    def __init__(self, provider: str = "anthropic") -> None:
        cfg = _load_config(provider)
        self._max_rpm: int = cfg["max_requests_per_minute"]
        self._max_tpm: int = cfg["max_tokens_per_minute"]
        self._est_tokens: int = cfg["estimated_tokens_per_request"]
        self._retry_base: float = cfg["retry_base_delay_seconds"]
        self._jitter: float = cfg["retry_jitter_fraction"]

        # Sliding window: list of UNIX timestamps for recent requests
        self._request_times: list[float] = []
        self._token_times: list[tuple[float, int]] = []  # (ts, tokens)
        self._lock = asyncio.Lock()

        self.min_interval: float = max(
            60.0 / self._max_rpm,
            self._est_tokens * 60.0 / self._max_tpm,
        )
        logger.info(
            "S3 RateLimiter: max_rpm=%d max_tpm=%d est_tok=%d → min_interval=%.2fs",
            self._max_rpm, self._max_tpm, self._est_tokens, self.min_interval,
        )

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        self._request_times = [t for t in self._request_times if t > cutoff]
        self._token_times = [(t, tok) for t, tok in self._token_times if t > cutoff]

    def _current_tpm(self) -> int:
        return sum(tok for _, tok in self._token_times)

    async def acquire(self, tokens: int | None = None) -> None:
        """Block until both RPM and TPM budgets allow next request."""
        tokens = tokens if tokens is not None else self._est_tokens
        async with self._lock:
            while True:
                now = time.monotonic()
                self._prune(now)

                rpm_ok = len(self._request_times) < self._max_rpm
                tpm_ok = self._current_tpm() + tokens <= self._max_tpm

                if rpm_ok and tpm_ok:
                    self._request_times.append(now)
                    self._token_times.append((now, tokens))
                    return

                # Compute how long until oldest entry expires
                oldest_req = self._request_times[0] if self._request_times else now
                oldest_tok = self._token_times[0][0] if self._token_times else now
                wait = min(oldest_req, oldest_tok) + 60.0 - now
                wait = max(wait, 0.01)

        # Release lock while sleeping so other coroutines can check
        await asyncio.sleep(wait)
        await self.acquire(tokens)

    async def backoff(self, attempt: int) -> None:
        """Exponential + jitter sleep after a 429. Call with attempt index (0-based)."""
        delay = self._retry_base * (2 ** attempt) * (1.0 + random.uniform(0, self._jitter))
        logger.warning(
            "S3 RateLimiter: 429 received (attempt %d) — backing off %.1fs",
            attempt, delay,
        )
        await asyncio.sleep(delay)
