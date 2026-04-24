"""Unit tests for RateLimiter (async sliding-window)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.enrichment.rate_limiter import RateLimiter, _load_config


class TestLoadConfig:
    def test_anthropic(self):
        cfg = _load_config("anthropic")
        assert "max_requests_per_minute" in cfg

    def test_groq(self):
        cfg = _load_config("groq")
        assert cfg["max_requests_per_minute"] > 0


class TestRateLimiterInit:
    def test_init_computes_min_interval(self):
        rl = RateLimiter("anthropic")
        assert rl.min_interval > 0
        assert rl._max_rpm > 0
        assert rl._max_tpm > 0

    def test_init_deepseek(self):
        rl = RateLimiter("deepseek")
        assert rl._max_rpm == 55


class TestAcquire:
    def test_acquire_succeeds_immediately(self):
        rl = RateLimiter("anthropic")
        asyncio.run(rl.acquire(tokens=100))
        assert len(rl._request_times) == 1
        assert rl._current_tpm() == 100

    def test_acquire_default_tokens(self):
        rl = RateLimiter("anthropic")
        asyncio.run(rl.acquire())
        assert len(rl._request_times) == 1

    def test_prune_removes_old_entries(self):
        rl = RateLimiter("anthropic")
        rl._request_times = [0.0, 1.0]
        rl._token_times = [(0.0, 100), (1.0, 100)]
        rl._prune(now=1000.0)
        assert rl._request_times == []
        assert rl._token_times == []

    def test_current_tpm_sums(self):
        rl = RateLimiter("anthropic")
        rl._token_times = [(1.0, 100), (2.0, 200)]
        assert rl._current_tpm() == 300


class TestBackoff:
    def test_backoff_calls_sleep(self):
        rl = RateLimiter("anthropic")

        async def run():
            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.return_value = None
                await rl.backoff(0)
                assert mock_sleep.called
                delay = mock_sleep.call_args[0][0]
                assert delay >= rl._retry_base

        asyncio.run(run())

    def test_backoff_grows_with_attempt(self):
        rl = RateLimiter("anthropic")
        delays = []

        async def run():
            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.return_value = None
                await rl.backoff(0)
                delays.append(mock_sleep.call_args[0][0])
                await rl.backoff(3)
                delays.append(mock_sleep.call_args[0][0])

        asyncio.run(run())
        assert delays[1] > delays[0]


class TestAcquireRateLimited:
    def test_acquire_waits_when_rpm_full(self):
        rl = RateLimiter("anthropic")
        # Fill the RPM budget with recent requests
        import time
        now = time.monotonic()
        rl._request_times = [now] * rl._max_rpm
        rl._token_times = [(now, 0) for _ in range(rl._max_rpm)]

        sleep_called = {"flag": False}

        async def fake_sleep(d):
            sleep_called["flag"] = True
            # drain the window so next iteration succeeds
            rl._request_times = []
            rl._token_times = []

        async def run():
            with patch("asyncio.sleep", side_effect=fake_sleep):
                await rl.acquire(tokens=100)

        asyncio.run(run())
        assert sleep_called["flag"] is True
